/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
 *
 * @param id Unique stage ID
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
 */
private[scheduler] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite)
  extends Logging {

  val numPartitions = rdd.partitions.length

  val inputSizes = Array.fill[Long](numPartitions)(0)
  val taskExecutionTimes = Array.fill[Long](numPartitions)(-1)
  val taskStartTimes = Array.fill[Long](numPartitions)(-1)
  val taskEndTimes = Array.fill[Long](numPartitions)(-1)

  val pipelineWidth = 4
  var startedPartitions = 4
  val partitionIdToCore = new HashMap[Int, Int]
  val lastFinishTime = Array.fill[Double](pipelineWidth)(0)
  val coreIdToPartition = Array.fill[Int](pipelineWidth)(-1)

  object LinearRegression {
    var n: Int = 0
    var sumx: Double = 0
    var sumy: Double = 0
    var sumx2: Double = 0
    var sumxy: Double = 0
    var a: Double = 0
    var b: Double = 0

    def taskEnd(partitionId: Int, endTime: Long): Unit = {
      val x = inputSizes(partitionId)
      val y = taskExecutionTimes(partitionId)
      n += 1
      sumx += x
      sumy += y
      sumx2 += x * x
      sumxy += x * y
      val xmean = sumx / n
      val ymean = sumy / n
      a = (n * sumxy - sumx * sumy) / (n * sumx2 - sumx * sumx)
      b = ymean - a * xmean

      taskEndTimes(partitionId) = endTime
      if (startedPartitions < numPartitions) {
        partitionIdToCore.put(startedPartitions, partitionIdToCore.get(partitionId) match {
          case Some(x) => x
          case None =>
            partitionIdToCore.put(partitionId, partitionId)
            partitionId
        })
        val coreId = partitionIdToCore(startedPartitions)
        lastFinishTime(coreId) = endTime
        coreIdToPartition(coreId) = startedPartitions
        startedPartitions += 1
      }
    }

    def taskStart(partitionId: Int, startTime: Long): Unit = {
      taskStartTimes(partitionId) = startTime
      if (partitionId < pipelineWidth) {
        lastFinishTime(partitionId) = startTime
        coreIdToPartition(partitionId) = partitionId
        partitionIdToCore.put(partitionId, partitionId)
      }
    }

    def predict(curTime: Long): Double = {
      /*var totalTime: Double = 0
      for ((inputSize, executionTime) <- (inputSizes zip taskExecutionTimes)) {
        if (executionTime == -1) {
          totalTime += a * inputSize + b
        }
      }
      for (i <- 0 until numPartitions) {
        if (taskStartTimes(i) != -1 && taskEndTimes(i) == -1)
          totalTime -= curTime - taskStartTimes(i)
      }
      (totalTime + curTime) / pipelineWidth*/
      val finishTime = lastFinishTime.clone()
      var nextPartition = 0
      for (i <- 0 until pipelineWidth) {
        finishTime(i) += a * inputSizes(coreIdToPartition(i)) + b
        nextPartition = math.max(nextPartition, coreIdToPartition(i))
      }
      nextPartition += 1
      for (i <- nextPartition until numPartitions) {
        var choose = 0
        for (j <- 1 until pipelineWidth) {
          if (finishTime(j) < finishTime(choose)) choose = j
        }
        finishTime(choose) += a * inputSizes(i) + b
      }
      finishTime.reduce(math.max(_, _))
    }
  }

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]

  val pendingPartitions = new HashSet[Int]

  /** The ID to use for the next new attempt for this stage. */
  private var nextAttemptId: Int = 0

  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  private var _internalAccumulators: Seq[Accumulator[Long]] = Seq.empty

  /** Internal accumulators shared across all tasks in this stage. */
  def internalAccumulators: Seq[Accumulator[Long]] = _internalAccumulators

  /**
   * Re-initialize the internal accumulators associated with this stage.
   *
   * This is called every time the stage is submitted, *except* when a subset of tasks
   * belonging to this stage has already finished. Otherwise, reinitializing the internal
   * accumulators here again will override partial values from the finished tasks.
   */
  def resetInternalAccumulators(): Unit = {
    _internalAccumulators = InternalAccumulator.create(rdd.sparkContext)
  }

  /**
   * Pointer to the [StageInfo] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   */
  private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

  /**
   * Set of stage attempt IDs that have failed with a FetchFailure. We keep track of these
   * failures in order to avoid endless retries if a stage keeps failing with a FetchFailure.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   */
  private val fetchFailedAttemptIds = new HashSet[Int]

  private[scheduler] def clearFailures() : Unit = {
    fetchFailedAttemptIds.clear()
  }

  /**
   * Check whether we should abort the failedStage due to multiple consecutive fetch failures.
   *
   * This method updates the running set of failed stage attempts and returns
   * true if the number of failures exceeds the allowable number of failures.
   */
  private[scheduler] def failedOnFetchAndShouldAbort(stageAttemptId: Int): Boolean = {
    fetchFailedAttemptIds.add(stageAttemptId)
    fetchFailedAttemptIds.size >= Stage.MAX_CONSECUTIVE_FETCH_FAILURES
  }

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), taskLocalityPreferences)
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  def findMissingPartitions(): Seq[Int]
}

private[scheduler] object Stage {
  // The number of consecutive failures allowed before a stage is aborted
  val MAX_CONSECUTIVE_FETCH_FAILURES = 4
}
