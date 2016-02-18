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

#include <string>
#include <queue>

#include <gmock/gmock.h>

#include <mesos/executor.hpp>

#include <mesos/v1/mesos.hpp>
#include <mesos/v1/resources.hpp>
#include <mesos/v1/scheduler.hpp>

#include <mesos/v1/scheduler/scheduler.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/gtest.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>
#include <process/queue.hpp>

#include <stout/lambda.hpp>
#include <stout/try.hpp>

#include "internal/devolve.hpp"
#include "internal/evolve.hpp"

#include "master/allocator/mesos/allocator.hpp"

#include "master/master.hpp"

#include "tests/containerizer.hpp"
#include "tests/mesos.hpp"

using mesos::internal::master::allocator::MesosAllocatorProcess;

using mesos::internal::master::Master;

using mesos::internal::slave::Containerizer;
using mesos::internal::slave::Slave;

using mesos::v1::scheduler::Call;
using mesos::v1::scheduler::Event;
using mesos::v1::scheduler::Mesos;

using process::Clock;
using process::Future;
using process::Owned;
using process::PID;
using process::Queue;

using std::string;

using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::Return;
using testing::WithParamInterface;

namespace mesos {
namespace internal {
namespace tests {


class SchedulerTest : public MesosTest, public WithParamInterface<ContentType>
{
protected:
  // Helper class for using EXPECT_CALL since the Mesos scheduler API
  // is callback based.
  class Callbacks
  {
  public:
    MOCK_METHOD0(connected, void());
    MOCK_METHOD0(disconnected, void());
    MOCK_METHOD1(received, void(const std::queue<Event>&));
  };
};


// The scheduler library tests are parameterized by the content type
// of the HTTP request.
INSTANTIATE_TEST_CASE_P(
    ContentType,
    SchedulerTest,
    ::testing::Values(ContentType::PROTOBUF, ContentType::JSON));


// Enqueues all received events into a libprocess queue.
ACTION_P(Enqueue, queue)
{
  std::queue<Event> events = arg0;
  while (!events.empty()) {
    // Note that we currently drop HEARTBEATs because most of these tests
    // are not designed to deal with heartbeats.
    // TODO(vinod): Implement DROP_HTTP_CALLS that can filter heartbeats.
    if (events.front().type() == Event::HEARTBEAT) {
      VLOG(1) << "Ignoring HEARTBEAT event";
    } else {
      queue->put(events.front());
    }
    events.pop();
  }
}


// This test verifies that when a scheduler resubscribes it receives
// SUBSCRIBED event with the previously assigned framework id.
TEST_P(SchedulerTest, Subscribe)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);
    subscribe->set_force(true);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  // Resubscribe with the same framework id.
  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);
    subscribe->mutable_framework_info()->mutable_id()->CopyFrom(id);
    subscribe->set_force(true);

    mesos.send(call);
  }

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());
  EXPECT_EQ(id, event.get().subscribed().framework_id());
}


TEST_P(SchedulerTest, TaskRunning)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;

  auto executor = std::make_shared<MockV1HTTPExecutor>();
  TestContainerizer containerizer(executorId, executor);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());

  EXPECT_CALL(*executor, connected(_))
    .WillOnce(executor::SendSubscribe(id, evolve(executorId)));

  EXPECT_CALL(*executor, subscribed(_, _));

  EXPECT_CALL(*executor, launch(_, _))
    .WillOnce(executor::SendUpdateFromTask(
        id, evolve(executorId), v1::TASK_RUNNING));

  Future<Nothing> acknowledged;
  EXPECT_CALL(*executor, acknowledged(_, _))
    .WillOnce(FutureSatisfy(&acknowledged));

  Future<Nothing> update;
  EXPECT_CALL(containerizer, update(_, _))
    .WillOnce(DoAll(FutureSatisfy(&update),
                    Return(Nothing())))
    .WillRepeatedly(Return(Future<Nothing>())); // Ignore subsequent calls.

  v1::TaskInfo taskInfo;
  taskInfo.set_name("");
  taskInfo.mutable_task_id()->set_value("1");
  taskInfo.mutable_agent_id()->CopyFrom(
      event.get().offers().offers(0).agent_id());
  taskInfo.mutable_resources()->CopyFrom(
      event.get().offers().offers(0).resources());
  taskInfo.mutable_executor()->CopyFrom(DEFAULT_V1_EXECUTOR_INFO);

  // TODO(benh): Enable just running a task with a command in the tests:
  //   taskInfo.mutable_command()->set_value("sleep 10");

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::ACCEPT);

    Call::Accept* accept = call.mutable_accept();
    accept->add_offer_ids()->CopyFrom(event.get().offers().offers(0).id());

    v1::Offer::Operation* operation = accept->add_operations();
    operation->set_type(v1::Offer::Operation::LAUNCH);
    operation->mutable_launch()->add_task_infos()->CopyFrom(taskInfo);

    mesos.send(call);
  }

  AWAIT_READY(acknowledged);

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::UPDATE, event.get().type());
  EXPECT_EQ(v1::TASK_RUNNING, event.get().update().status().state());
  EXPECT_TRUE(event.get().update().status().has_executor_id());
  EXPECT_EQ(executorId, devolve(event.get().update().status().executor_id()));

  AWAIT_READY(update);

  EXPECT_CALL(*executor, shutdown(_, _))
    .Times(AtMost(1));

  EXPECT_CALL(*executor, disconnected(_))
    .Times(AtMost(1));
}


TEST_P(SchedulerTest, ReconcileTask)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;

  auto executor = std::make_shared<MockV1HTTPExecutor>();
  TestContainerizer containerizer(executorId, executor);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());

  EXPECT_CALL(*executor, connected(_))
    .WillOnce(executor::SendSubscribe(id, evolve(executorId)));

  EXPECT_CALL(*executor, subscribed(_, _));

  EXPECT_CALL(*executor, launch(_, _))
    .WillOnce(executor::SendUpdateFromTask(
        id, evolve(executorId), v1::TASK_RUNNING));

  Future<Nothing> acknowledged;
  EXPECT_CALL(*executor, acknowledged(_, _))
    .WillOnce(FutureSatisfy(&acknowledged));

  v1::Offer offer = event.get().offers().offers(0);

  v1::TaskInfo taskInfo =
    evolve(createTask(devolve(offer), "", DEFAULT_EXECUTOR_ID));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::ACCEPT);

    Call::Accept* accept = call.mutable_accept();
    accept->add_offer_ids()->CopyFrom(offer.id());

    v1::Offer::Operation* operation = accept->add_operations();
    operation->set_type(v1::Offer::Operation::LAUNCH);
    operation->mutable_launch()->add_task_infos()->CopyFrom(taskInfo);

    mesos.send(call);
  }

  AWAIT_READY(acknowledged);

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::UPDATE, event.get().type());
  EXPECT_EQ(v1::TASK_RUNNING, event.get().update().status().state());

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::RECONCILE);

    Call::Reconcile::Task* task = call.mutable_reconcile()->add_tasks();
    task->mutable_task_id()->CopyFrom(taskInfo.task_id());

    mesos.send(call);
  }

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::UPDATE, event.get().type());
  EXPECT_FALSE(event.get().update().status().has_uuid());
  EXPECT_EQ(v1::TASK_RUNNING, event.get().update().status().state());
  EXPECT_EQ(v1::TaskStatus::REASON_RECONCILIATION,
            event.get().update().status().reason());

  EXPECT_CALL(*executor, shutdown(_, _))
    .Times(AtMost(1));

  EXPECT_CALL(*executor, disconnected(_))
    .Times(AtMost(1));
}


TEST_P(SchedulerTest, KillTask)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;

  auto executor = std::make_shared<MockV1HTTPExecutor>();
  TestContainerizer containerizer(executorId, executor);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());

  EXPECT_CALL(*executor, connected(_))
    .WillOnce(executor::SendSubscribe(id, evolve(executorId)));

  EXPECT_CALL(*executor, subscribed(_, _));

  EXPECT_CALL(*executor, launch(_, _))
    .WillOnce(executor::SendUpdateFromTask(
        id, evolve(executorId), v1::TASK_RUNNING));

  Future<Nothing> acknowledged;
  EXPECT_CALL(*executor, acknowledged(_, _))
    .WillOnce(FutureSatisfy(&acknowledged))
    .WillRepeatedly(Return());

  v1::Offer offer = event.get().offers().offers(0);

  v1::TaskInfo taskInfo =
    evolve(createTask(devolve(offer), "", DEFAULT_EXECUTOR_ID));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::ACCEPT);

    Call::Accept* accept = call.mutable_accept();
    accept->add_offer_ids()->CopyFrom(offer.id());

    v1::Offer::Operation* operation = accept->add_operations();
    operation->set_type(v1::Offer::Operation::LAUNCH);
    operation->mutable_launch()->add_task_infos()->CopyFrom(taskInfo);

    mesos.send(call);
  }

  AWAIT_READY(acknowledged);

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::UPDATE, event.get().type());
  EXPECT_EQ(v1::TASK_RUNNING, event.get().update().status().state());

  {
    // Acknowledge TASK_RUNNING update.
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::ACKNOWLEDGE);

    Call::Acknowledge* acknowledge = call.mutable_acknowledge();
    acknowledge->mutable_task_id()->CopyFrom(taskInfo.task_id());
    acknowledge->mutable_agent_id()->CopyFrom(offer.agent_id());
    acknowledge->set_uuid(event.get().update().status().uuid());

    mesos.send(call);
  }

  EXPECT_CALL(*executor, kill(_, _))
    .WillOnce(executor::SendUpdateFromTaskID(
        id, evolve(executorId), v1::TASK_KILLED));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::KILL);

    Call::Kill* kill = call.mutable_kill();
    kill->mutable_task_id()->CopyFrom(taskInfo.task_id());
    kill->mutable_agent_id()->CopyFrom(offer.agent_id());

    mesos.send(call);
  }

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::UPDATE, event.get().type());
  EXPECT_EQ(v1::TASK_KILLED, event.get().update().status().state());

  EXPECT_CALL(*executor, shutdown(_, _))
    .Times(AtMost(1));

  EXPECT_CALL(*executor, disconnected(_))
    .Times(AtMost(1));
}


TEST_P(SchedulerTest, ShutdownExecutor)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;

  auto executor = std::make_shared<MockV1HTTPExecutor>();
  TestContainerizer containerizer(executorId, executor);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());

  EXPECT_CALL(*executor, connected(_))
    .WillOnce(executor::SendSubscribe(id, evolve(executorId)));

  EXPECT_CALL(*executor, subscribed(_, _));

  EXPECT_CALL(*executor, launch(_, _))
    .WillOnce(executor::SendUpdateFromTask(
        id, evolve(executorId), v1::TASK_FINISHED));

  Future<Nothing> acknowledged;
  EXPECT_CALL(*executor, acknowledged(_, _))
    .WillOnce(FutureSatisfy(&acknowledged));

  v1::Offer offer = event.get().offers().offers(0);

  v1::TaskInfo taskInfo =
    evolve(createTask(devolve(offer), "", DEFAULT_EXECUTOR_ID));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::ACCEPT);

    Call::Accept* accept = call.mutable_accept();
    accept->add_offer_ids()->CopyFrom(offer.id());

    v1::Offer::Operation* operation = accept->add_operations();
    operation->set_type(v1::Offer::Operation::LAUNCH);
    operation->mutable_launch()->add_task_infos()->CopyFrom(taskInfo);

    mesos.send(call);
  }

  AWAIT_READY(acknowledged);

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::UPDATE, event.get().type());
  EXPECT_EQ(v1::TASK_FINISHED, event.get().update().status().state());

  Future<Nothing> shutdown;
  EXPECT_CALL(*executor, shutdown(_, _))
    .WillOnce(FutureSatisfy(&shutdown));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::SHUTDOWN);

    Call::Shutdown* shutdown = call.mutable_shutdown();
    shutdown->mutable_executor_id()->CopyFrom(DEFAULT_V1_EXECUTOR_ID);
    shutdown->mutable_agent_id()->CopyFrom(offer.agent_id());

    mesos.send(call);
  }

  AWAIT_READY(shutdown);
  containerizer.destroy(devolve(id), executorId);

  // Executor termination results in a 'FAILURE' event.
  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::FAILURE, event.get().type());
  EXPECT_EQ(evolve(executorId), event.get().failure().executor_id());
}


TEST_P(SchedulerTest, Teardown)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;

  auto executor = std::make_shared<MockV1HTTPExecutor>();
  TestContainerizer containerizer(executorId, executor);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());

  EXPECT_CALL(*executor, connected(_))
    .WillOnce(executor::SendSubscribe(id, evolve(executorId)));

  EXPECT_CALL(*executor, subscribed(_, _));

  EXPECT_CALL(*executor, launch(_, _))
    .WillOnce(executor::SendUpdateFromTask(
        id, evolve(executorId), v1::TASK_RUNNING));

  Future<Nothing> acknowledged;
  EXPECT_CALL(*executor, acknowledged(_, _))
    .WillOnce(FutureSatisfy(&acknowledged));

  v1::Offer offer = event.get().offers().offers(0);

  v1::TaskInfo taskInfo =
    evolve(createTask(devolve(offer), "", DEFAULT_EXECUTOR_ID));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::ACCEPT);

    Call::Accept* accept = call.mutable_accept();
    accept->add_offer_ids()->CopyFrom(offer.id());

    v1::Offer::Operation* operation = accept->add_operations();
    operation->set_type(v1::Offer::Operation::LAUNCH);
    operation->mutable_launch()->add_task_infos()->CopyFrom(taskInfo);

    mesos.send(call);
  }

  AWAIT_READY(acknowledged);

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::UPDATE, event.get().type());
  EXPECT_EQ(v1::TASK_RUNNING, event.get().update().status().state());

  Future<Nothing> shutdown;
  EXPECT_CALL(*executor, shutdown(_, _))
    .WillOnce(FutureSatisfy(&shutdown));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::TEARDOWN);

    mesos.send(call);
  }

  AWAIT_READY(shutdown);
}


TEST_P(SchedulerTest, Decline)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get());

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  ASSERT_EQ(1, event.get().offers().offers().size());

  v1::Offer offer = event.get().offers().offers(0);
  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::DECLINE);

    Call::Decline* decline = call.mutable_decline();
    decline->add_offer_ids()->CopyFrom(offer.id());

    // Set 0s filter to immediately get another offer.
    v1::Filters filters;
    filters.set_refuse_seconds(0);
    decline->mutable_filters()->CopyFrom(filters);

    mesos.send(call);
  }

  // If the resources were properly declined, the scheduler should
  // get another offer with same amount of resources.
  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  ASSERT_EQ(1, event.get().offers().offers().size());
  ASSERT_EQ(offer.resources(), event.get().offers().offers(0).resources());
}


TEST_P(SchedulerTest, Revive)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get());

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());

  v1::Offer offer = event.get().offers().offers(0);
  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::DECLINE);

    Call::Decline* decline = call.mutable_decline();
    decline->add_offer_ids()->CopyFrom(offer.id());

    // Set 1hr filter to not immediately get another offer.
    v1::Filters filters;
    filters.set_refuse_seconds(Hours(1).secs());
    decline->mutable_filters()->CopyFrom(filters);

    mesos.send(call);
  }

  // No offers should be sent within 30 mins because we set a filter
  // for 1 hr.
  Clock::pause();
  Clock::advance(Minutes(30));
  Clock::settle();

  event = events.get();
  ASSERT_TRUE(event.isPending());

  // On revival the filters should be cleared and the scheduler should
  // get another offer with same amount of resources.
  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::REVIVE);

    mesos.send(call);
  }

  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());
  ASSERT_EQ(offer.resources(), event.get().offers().offers(0).resources());
}


TEST_P(SchedulerTest, Suppress)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get());

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());

  v1::Offer offer = event.get().offers().offers(0);
  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::DECLINE);

    Call::Decline* decline = call.mutable_decline();
    decline->add_offer_ids()->CopyFrom(offer.id());

    // Set 1hr filter to not immediately get another offer.
    v1::Filters filters;
    filters.set_refuse_seconds(Hours(1).secs());
    decline->mutable_filters()->CopyFrom(filters);

    mesos.send(call);
  }

  Future<Nothing> suppressOffers =
    FUTURE_DISPATCH(_, &MesosAllocatorProcess::suppressOffers);

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::SUPPRESS);

    mesos.send(call);
  }

  AWAIT_READY(suppressOffers);

  // Wait for allocator to finish executing 'suppressOffers()'.
  Clock::pause();
  Clock::settle();

  // No offers should be sent within 100 mins because the framework
  // suppressed offers.
  Clock::advance(Minutes(100));
  Clock::settle();

  event = events.get();
  ASSERT_TRUE(event.isPending());

  // On reviving offers the scheduler should get another offer with same amount
  // of resources.
  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::REVIVE);

    mesos.send(call);
  }

  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());
  ASSERT_EQ(offer.resources(), event.get().offers().offers(0).resources());
}


TEST_P(SchedulerTest, Message)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  ExecutorID executorId = DEFAULT_EXECUTOR_ID;

  auto executor = std::make_shared<MockV1HTTPExecutor>();
  TestContainerizer containerizer(executorId, executor);

  Owned<MasterDetector> detector = master->detector();
  Owned<cluster::Slave> slave = StartSlave(detector.get(), &containerizer);

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::OFFERS, event.get().type());
  EXPECT_NE(0, event.get().offers().offers().size());

  EXPECT_CALL(*executor, connected(_))
    .WillOnce(executor::SendSubscribe(id, evolve(executorId)));

  EXPECT_CALL(*executor, subscribed(_, _));

  EXPECT_CALL(*executor, launch(_, _))
    .WillOnce(executor::SendUpdateFromTask(
        id, evolve(executorId), v1::TASK_RUNNING));

  Future<Nothing> acknowledged;
  EXPECT_CALL(*executor, acknowledged(_, _))
    .WillOnce(FutureSatisfy(&acknowledged));

  v1::Offer offer = event.get().offers().offers(0);

  v1::TaskInfo taskInfo =
    evolve(createTask(devolve(offer), "", DEFAULT_EXECUTOR_ID));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::ACCEPT);

    Call::Accept* accept = call.mutable_accept();
    accept->add_offer_ids()->CopyFrom(offer.id());

    v1::Offer::Operation* operation = accept->add_operations();
    operation->set_type(v1::Offer::Operation::LAUNCH);
    operation->mutable_launch()->add_task_infos()->CopyFrom(taskInfo);

    mesos.send(call);
  }

  AWAIT_READY(acknowledged);

  event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::UPDATE, event.get().type());
  EXPECT_EQ(v1::TASK_RUNNING, event.get().update().status().state());

  Future<v1::executor::Event::Message> message;
  EXPECT_CALL(*executor, message(_, _))
    .WillOnce(FutureArg<1>(&message));

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::MESSAGE);

    Call::Message* message = call.mutable_message();
    message->mutable_agent_id()->CopyFrom(offer.agent_id());
    message->mutable_executor_id()->CopyFrom(DEFAULT_V1_EXECUTOR_ID);
    message->set_data("hello world");

    mesos.send(call);
  }

  AWAIT_READY(message);
  ASSERT_EQ("hello world", message->data());

  EXPECT_CALL(*executor, shutdown(_, _))
    .Times(AtMost(1));

  EXPECT_CALL(*executor, disconnected(_))
    .Times(AtMost(1));
}


TEST_P(SchedulerTest, Request)
{
  // NOTE: The callbacks and event queue must be stack allocated below
  // the master, as the master may send heartbeats during destruction.
  Callbacks callbacks;
  Queue<Event> events;

  master::Flags flags = CreateMasterFlags();
  flags.authenticate_frameworks = false;

  Owned<cluster::Master> master = StartMaster(flags);

  Future<Nothing> connected;
  EXPECT_CALL(callbacks, connected())
    .WillOnce(FutureSatisfy(&connected));

  Mesos mesos(
      master->pid,
      GetParam(),
      lambda::bind(&Callbacks::connected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::disconnected, lambda::ref(callbacks)),
      lambda::bind(&Callbacks::received, lambda::ref(callbacks), lambda::_1));

  AWAIT_READY(connected);

  EXPECT_CALL(callbacks, received(_))
    .WillRepeatedly(Enqueue(&events));

  {
    Call call;
    call.set_type(Call::SUBSCRIBE);

    Call::Subscribe* subscribe = call.mutable_subscribe();
    subscribe->mutable_framework_info()->CopyFrom(DEFAULT_V1_FRAMEWORK_INFO);

    mesos.send(call);
  }

  Future<Event> event = events.get();
  AWAIT_READY(event);
  EXPECT_EQ(Event::SUBSCRIBED, event.get().type());

  v1::FrameworkID id(event.get().subscribed().framework_id());

  Future<Nothing> requestResources =
    FUTURE_DISPATCH(_, &MesosAllocatorProcess::requestResources);

  {
    Call call;
    call.mutable_framework_id()->CopyFrom(id);
    call.set_type(Call::REQUEST);

    // Create a dummy request.
    Call::Request* request = call.mutable_request();
    request->add_requests();

    mesos.send(call);
  }

  AWAIT_READY(requestResources);
}


// TODO(benh): Write test for sending Call::Acknowledgement through
// master to slave when Event::Update was generated locally.

} // namespace tests {
} // namespace internal {
} // namespace mesos {
