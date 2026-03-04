// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <utility>

#include "ray/observability/ray_event_interface.h"
#include "ray/util/logging.h"

namespace ray {
namespace observability {

/// Wraps an existing RayEvent proto so it can be passed through RayEventRecorder.
class RayEventProtoWrapper : public RayEventInterface {
 public:
  explicit RayEventProtoWrapper(rpc::events::RayEvent event) : event_(std::move(event)) {}

  std::string GetEntityId() const override { return event_.event_id(); }

  void Merge(RayEventInterface &&other) override {
    (void)other;
    RAY_CHECK(false) << "RayEventProtoWrapper does not support merge.";
  }

  rpc::events::RayEvent Serialize() && override { return std::move(event_); }

  rpc::events::RayEvent::EventType GetEventType() const override {
    return event_.event_type();
  }

  bool SupportsMerge() const override { return false; }

 private:
  rpc::events::RayEvent event_;
};

}  // namespace observability
}  // namespace ray
