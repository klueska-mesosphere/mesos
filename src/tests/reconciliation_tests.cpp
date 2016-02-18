// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdint.h>
#include <unistd.h>

#include <vector>

#include <gmock/gmock.h>

#include <mesos/mesos.hpp>
#include <mesos/scheduler.hpp>

#include <mesos/scheduler/scheduler.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>
#include <process/process.hpp>

#include <stout/uuid.hpp>

#include "common/protobuf_utils.hpp"

#include "master/flags.hpp"
#include "master/master.hpp"

#include "slave/slave.hpp"

#include "tests/containerizer.hpp"
#include "tests/mesos.hpp"

using mesos::internal::master::Master;

using mesos::internal::slave::Slave;

using process::Clock;
using process::Future;
using process::Owned;
using process::PID;
using process::Promise;

using std::vector;

using testing::_;
using testing::An;
using testing::AtMost;
using testing::DoAll;
using testing::Return;
using testing::SaveArg;

namespace mesos {
namespace internal {
namespace tests {

class ReconciliationTest : public MesosTest {};


// This test verifies that reconciliation sends the latest task
// status, when the task state does not match between the framework
// and the master.
TEST_F(ReconciliationTest, TaskStateMismatch)
{
  Owned<cluster::Master> master = StartMaster();

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(LaunchTasks(DEFAULT_EXECUTOR_INFO, 1, 1, 512, "*"))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  driver.start();

  // Wait until the framework is registered.
  AWAIT_READY(frameworkId);

  AWAIT_READY(update);
  EXPECT_EQ(TASK_RUNNING, update.get().state());

  EXPECT_EQ(true, update.get().has_slave_id());

  const TaskID taskId = update.get().task_id();
  const SlaveID slaveId = update.get().slave_id();

  // If framework has different state, current state should be reported.
  Future<TaskStatus> update2;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update2));

  vector<TaskStatus> statuses;

  TaskStatus status;
  status.mutable_task_id()->CopyFrom(taskId);
  status.mutable_slave_id()->CopyFrom(slaveId);
  status.set_state(TASK_KILLED);

  statuses.push_back(status);

  driver.reconcileTasks(statuses);

  AWAIT_READY(update2);
  EXPECT_EQ(TASK_RUNNING, update2.get().state());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// This test verifies that task reconciliation results in a status
// update, when the task state matches between the framework and the
// master.
// TODO(bmahler): Now that the semantics have changed, consolidate
// these tests? There's no need to test anything related to the
// task state difference between the master and the framework.
TEST_F(ReconciliationTest, TaskStateMatch)
{
  Owned<cluster::Master> master = StartMaster();

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(LaunchTasks(DEFAULT_EXECUTOR_INFO, 1, 1, 512, "*"))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  driver.start();

  // Wait until the framework is registered.
  AWAIT_READY(frameworkId);

  AWAIT_READY(update);
  EXPECT_EQ(TASK_RUNNING, update.get().state());

  EXPECT_EQ(true, update.get().has_slave_id());

  const TaskID taskId = update.get().task_id();
  const SlaveID slaveId = update.get().slave_id();

  // Framework should not receive a status update.
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .Times(0);

  vector<TaskStatus> statuses;

  TaskStatus status;
  status.mutable_task_id()->CopyFrom(taskId);
  status.mutable_slave_id()->CopyFrom(slaveId);
  status.set_state(TASK_RUNNING);

  statuses.push_back(status);

  Future<TaskStatus> update2;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update2));

  driver.reconcileTasks(statuses);

  AWAIT_READY(update2);
  EXPECT_EQ(TASK_RUNNING, update2.get().state());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// This test verifies that reconciliation of a task that belongs to an
// unknown slave results in TASK_LOST.
TEST_F(ReconciliationTest, UnknownSlave)
{
  Owned<cluster::Master> master = StartMaster();

  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  driver.start();

  // Wait until the framework is registered.
  AWAIT_READY(frameworkId);

  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  vector<TaskStatus> statuses;

  // Create a task status with a random slave id (and task id).
  TaskStatus status;
  status.mutable_task_id()->set_value(UUID::random().toString());
  status.mutable_slave_id()->set_value(UUID::random().toString());
  status.set_state(TASK_RUNNING);

  statuses.push_back(status);

  driver.reconcileTasks(statuses);

  // Framework should receive TASK_LOST because the slave is unknown.
  AWAIT_READY(update);
  EXPECT_EQ(TASK_LOST, update.get().state());

  driver.stop();
  driver.join();
}


// This test verifies that reconciliation of an unknown task that
// belongs to a known slave results in TASK_LOST.
TEST_F(ReconciliationTest, UnknownTask)
{
  Owned<cluster::Master> master = StartMaster();

  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get());

  // Wait for the slave to register and get the slave id.
  AWAIT_READY(slaveRegisteredMessage);
  const SlaveID slaveId = slaveRegisteredMessage.get().slave_id();

  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillRepeatedly(Return()); // Ignore offers.

  driver.start();

  // Wait until the framework is registered.
  AWAIT_READY(frameworkId);

  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  vector<TaskStatus> statuses;

  // Create a task status with a random task id.
  TaskStatus status;
  status.mutable_task_id()->set_value(UUID::random().toString());
  status.mutable_slave_id()->CopyFrom(slaveId);
  status.set_state(TASK_RUNNING);

  statuses.push_back(status);

  driver.reconcileTasks(statuses);

  // Framework should receive TASK_LOST for unknown task.
  AWAIT_READY(update);
  EXPECT_EQ(TASK_LOST, update.get().state());

  driver.stop();
  driver.join();
}


// This test verifies that the killTask request of an unknown task
// results in reconciliation. In this case, the task is unknown
// and there are no transitional slaves.
TEST_F(ReconciliationTest, UnknownKillTask)
{
  Owned<cluster::Master> master = StartMaster();

  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  driver.start();

  // Wait until the framework is registered.
  AWAIT_READY(frameworkId);

  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  vector<TaskStatus> statuses;

  // Create a task status with a random task id.
  TaskID taskId;
  taskId.set_value(UUID::random().toString());

  driver.killTask(taskId);

  // Framework should receive TASK_LOST for unknown task.
  AWAIT_READY(update);
  EXPECT_EQ(TASK_LOST, update.get().state());

  driver.stop();
  driver.join();
}


// This test verifies that reconciliation of a task that belongs to a
// slave that is a transitional state doesn't result in an update.
TEST_F(ReconciliationTest, SlaveInTransition)
{
  master::Flags masterFlags = CreateMasterFlags();
  Owned<cluster::Master> master = StartMaster();

  // Reuse slaveFlags so both StartSlave() use the same work_dir.
  slave::Flags slaveFlags = CreateSlaveFlags();

  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), slaveFlags);

  // Wait for the slave to register and get the slave id.
  AWAIT_READY(slaveRegisteredMessage);
  const SlaveID slaveId = slaveRegisteredMessage.get().slave_id();

  // Stop the slave and master (a bit later).
  slave->terminate();
  slave.reset();

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillRepeatedly(Return()); // Ignore offers.

  // Framework should not receive any update.
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .Times(0);

  // Drop '&Master::_reregisterSlave' dispatch so that the slave is
  // in 'reregistering' state.
  Future<Nothing> _reregisterSlave =
    DROP_DISPATCH(_, &Master::_reregisterSlave);

  // Restart the master.
  master.reset();
  master = StartMaster(masterFlags);

  driver.start();

  // Wait for the framework to register.
  AWAIT_READY(frameworkId);

  // Restart the slave.
  detector = master->detector();
  slave = StartSlave(detector.get(), slaveFlags);

  // Slave will be in 'reregistering' state here.
  AWAIT_READY(_reregisterSlave);

  vector<TaskStatus> statuses;

  // Create a task status with a random task id.
  TaskStatus status;
  status.mutable_task_id()->set_value(UUID::random().toString());
  status.mutable_slave_id()->CopyFrom(slaveId);
  status.set_state(TASK_RUNNING);

  statuses.push_back(status);

  Future<mesos::scheduler::Call> reconcileCall = FUTURE_CALL(
      mesos::scheduler::Call(), mesos::scheduler::Call::RECONCILE, _ , _);

  Clock::pause();

  driver.reconcileTasks(statuses);

  // Make sure the master received the reconcile call.
  AWAIT_READY(reconcileCall);

  // The Clock::settle() will ensure that framework would receive
  // a status update if it is sent by the master. In this test it
  // shouldn't receive any.
  Clock::settle();

  driver.stop();
  driver.join();
}


// This test ensures that an implicit reconciliation request results
// in updates for all non-terminal tasks known to the master.
TEST_F(ReconciliationTest, ImplicitNonTerminalTask)
{
  Owned<cluster::Master> master = StartMaster();

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  // Launch a framework and get a task running.
  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(LaunchTasks(DEFAULT_EXECUTOR_INFO, 1, 1, 512, "*"))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  driver.start();

  // Wait until the framework is registered.
  AWAIT_READY(frameworkId);

  AWAIT_READY(update);
  EXPECT_EQ(TASK_RUNNING, update.get().state());
  EXPECT_TRUE(update.get().has_slave_id());

  // When making an implicit reconciliation request, the non-terminal
  // task should be sent back.
  Future<TaskStatus> update2;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update2));

  vector<TaskStatus> statuses;
  driver.reconcileTasks(statuses);

  AWAIT_READY(update2);
  EXPECT_EQ(TASK_RUNNING, update2.get().state());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// This test ensures that the master does not send updates for
// terminal tasks during an implicit reconciliation request.
// TODO(bmahler): Soon the master will keep non-acknowledged
// tasks, and this test may break.
TEST_F(ReconciliationTest, ImplicitTerminalTask)
{
  Owned<cluster::Master> master = StartMaster();

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  // Launch a framework and get a task terminal.
  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(LaunchTasks(DEFAULT_EXECUTOR_INFO, 1, 1, 512, "*"))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_FINISHED));

  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  driver.start();

  // Wait until the framework is registered.
  AWAIT_READY(frameworkId);

  AWAIT_READY(update);
  EXPECT_EQ(TASK_FINISHED, update.get().state());
  EXPECT_TRUE(update.get().has_slave_id());

  // Framework should not receive any further updates.
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .Times(0);

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  Future<mesos::scheduler::Call> reconcileCall = FUTURE_CALL(
      mesos::scheduler::Call(), mesos::scheduler::Call::RECONCILE, _ , _);

  Clock::pause();

  // When making an implicit reconciliation request, the master
  // should not send back terminal tasks.
  vector<TaskStatus> statuses;
  driver.reconcileTasks(statuses);

  // Make sure the master received the reconcile call.
  AWAIT_READY(reconcileCall);

  // The Clock::settle() will ensure that framework would receive
  // a status update if it is sent by the master. In this test it
  // shouldn't receive any.
  Clock::settle();

  driver.stop();
  driver.join();
}


// This test ensures that reconciliation requests for tasks that are
// pending are exposed in reconciliation.
TEST_F(ReconciliationTest, PendingTask)
{
  MockAuthorizer authorizer;
  Owned<cluster::Master> master = StartMaster(&authorizer);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Future<SlaveRegisteredMessage> slaveRegisteredMessage =
    FUTURE_PROTOBUF(SlaveRegisteredMessage(), _, _);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get());

  // Wait for the slave to register and get the slave id.
  AWAIT_READY(slaveRegisteredMessage);
  const SlaveID slaveId = slaveRegisteredMessage.get().slave_id();

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _))
    .Times(1);

  Future<vector<Offer> > offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());

  // Return a pending future from authorizer.
  Future<Nothing> authorize;
  Promise<bool> promise;
  EXPECT_CALL(authorizer, authorize(An<const mesos::ACL::RunTask&>()))
    .WillOnce(DoAll(FutureSatisfy(&authorize),
                    Return(promise.future())));

  TaskInfo task = createTask(offers.get()[0], "", DEFAULT_EXECUTOR_ID);

  driver.launchTasks(offers.get()[0].id(), {task});

  // Wait until authorization is in progress.
  AWAIT_READY(authorize);

  // First send an implicit reconciliation request for this task.
  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  vector<TaskStatus> statuses;
  driver.reconcileTasks(statuses);

  AWAIT_READY(update);
  EXPECT_EQ(TASK_STAGING, update.get().state());
  EXPECT_TRUE(update.get().has_slave_id());

  // Now send an explicit reconciliation request for this task.
  Future<TaskStatus> update2;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update2));

  TaskStatus status;
  status.mutable_task_id()->CopyFrom(task.task_id());
  status.mutable_slave_id()->CopyFrom(slaveId);
  status.set_state(TASK_STAGING);
  statuses.push_back(status);

  driver.reconcileTasks(statuses);

  AWAIT_READY(update2);
  EXPECT_EQ(TASK_STAGING, update2.get().state());
  EXPECT_TRUE(update2.get().has_slave_id());

  driver.stop();
  driver.join();
}


// This test ensures that the master responds with the latest state
// for tasks that are terminal at the master, but have not been
// acknowledged by the framework. See MESOS-1389.
TEST_F(ReconciliationTest, UnacknowledgedTerminalTask)
{
  Owned<cluster::Master> master = StartMaster();

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  // Launch a framework and get a task into a terminal state.
  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master->pid, DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(LaunchTasks(DEFAULT_EXECUTOR_INFO, 1, 1, 512, "*"))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_FINISHED));

  Future<TaskStatus> update1;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update1));

  // Prevent the slave from retrying the status update by
  // only allowing a single update through to the master.
  DROP_PROTOBUFS(StatusUpdateMessage(), _, master->pid);
  FUTURE_PROTOBUF(StatusUpdateMessage(), _, master->pid);

  // Drop the status update acknowledgements to ensure that the
  // task remains terminal and unacknowledged in the master.
  DROP_CALLS(mesos::scheduler::Call(),
             mesos::scheduler::Call::ACKNOWLEDGE,
             _,
             master->pid);

  driver.start();

  // Wait until the framework is registered.
  AWAIT_READY(frameworkId);

  AWAIT_READY(update1);
  EXPECT_EQ(TASK_FINISHED, update1.get().state());
  EXPECT_TRUE(update1.get().has_slave_id());

  // Framework should receive a TASK_FINISHED update, since the
  // master did not receive the acknowledgement.
  Future<TaskStatus> update2;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update2));

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  vector<TaskStatus> statuses;
  driver.reconcileTasks(statuses);

  AWAIT_READY(update2);
  EXPECT_EQ(TASK_FINISHED, update2.get().state());
  EXPECT_TRUE(update2.get().has_slave_id());

  driver.stop();
  driver.join();
}


// This test verifies that when the task's latest and status update
// states differ, master responds to reconciliation request with the
// status update state.
TEST_F(ReconciliationTest, ReconcileStatusUpdateTaskState)
{
  // Start a master.
  Owned<cluster::Master> master = StartMaster();

  // Start a slave.
  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);
  StandaloneMasterDetector slaveDetector(master->pid);
  Owned<cluster::Slave> slave = StartSlave(&slaveDetector, &containerizer);

  // Start a scheduler.
  MockScheduler sched;
  StandaloneMasterDetector schedulerDetector(master->pid);
  TestingMesosSchedulerDriver driver(&sched, &schedulerDetector);

  EXPECT_CALL(sched, registered(&driver, _, _));

  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(LaunchTasks(DEFAULT_EXECUTOR_INFO, 1, 2, 1024, "*"))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  ExecutorDriver* execDriver;
  EXPECT_CALL(exec, registered(_, _, _, _))
    .WillOnce(SaveArg<0>(&execDriver));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  // Signal when the first update is dropped.
  Future<StatusUpdateMessage> statusUpdateMessage =
    DROP_PROTOBUF(StatusUpdateMessage(), _, master->pid);

  Future<Nothing> ___statusUpdate = FUTURE_DISPATCH(_, &Slave::___statusUpdate);

  driver.start();

  // Pause the clock to avoid status update retries.
  Clock::pause();

  // Wait until TASK_RUNNING is sent to the master.
  AWAIT_READY(statusUpdateMessage);

  // Ensure status update manager handles TASK_RUNNING update.
  AWAIT_READY(___statusUpdate);

  Future<Nothing> ___statusUpdate2 =
    FUTURE_DISPATCH(_, &Slave::___statusUpdate);

  // Now send TASK_FINISHED update.
  TaskStatus finishedStatus;
  finishedStatus = statusUpdateMessage.get().update().status();
  finishedStatus.set_state(TASK_FINISHED);
  execDriver->sendStatusUpdate(finishedStatus);

  // Ensure status update manager handles TASK_FINISHED update.
  AWAIT_READY(___statusUpdate2);

  EXPECT_CALL(sched, disconnected(&driver))
    .WillOnce(Return());

  // Simulate master failover by restarting the master.
  master.reset();
  master = StartMaster();

  Clock::resume();

  Future<Nothing> registered;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureSatisfy(&registered));

  // Re-register the framework.
  schedulerDetector.appoint(master->pid);

  AWAIT_READY(registered);

  Future<SlaveReregisteredMessage> slaveReregisteredMessage =
    FUTURE_PROTOBUF(SlaveReregisteredMessage(), master->pid, slave->pid);

  // Drop all updates to the second master.
  DROP_PROTOBUFS(StatusUpdateMessage(), _, master->pid);

  // Re-register the slave.
  slaveDetector.appoint(master->pid);

  AWAIT_READY(slaveReregisteredMessage);

  // Framework should receive a TASK_RUNNING update, since that is the
  // latest status update state of the task.
  Future<TaskStatus> update;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&update));

  // Reconcile the state of the task.
  vector<TaskStatus> statuses;
  driver.reconcileTasks(statuses);

  AWAIT_READY(update);
  EXPECT_EQ(TASK_RUNNING, update.get().state());

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
