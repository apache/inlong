/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>
#include <inlong_api.h>
#include <atomic>
#include <thread>
#include <iostream>
#include <mutex>

namespace py = pybind11;

std::map<inlong::UserCallBack, py::function> g_py_callbacks;
std::atomic<bool> stop_callbacks(false);
std::mutex callback_mutex;

int UserCallBackBridge(const char *a, const char *b, const char *c, int32_t d, const int64_t e, const char *f) {
    std::unique_lock<std::mutex> lock(callback_mutex);
    if (stop_callbacks) {
        return -1;
    }
    auto it = g_py_callbacks.find(UserCallBackBridge);
    if (it != g_py_callbacks.end()) {
        if (stop_callbacks) {
            return -1;
        }
        py::gil_scoped_acquire acquire;
        if (stop_callbacks) {
            return -1;
        }
        int result = it->second(a, b, c, d, e, f).cast<int>();
        py::gil_scoped_release release;
        return result;
    }
    return -1;
}

PYBIND11_MODULE(inlong_dataproxy, m) {
    m.doc() = "Python bindings for InLong SDK API";
    py::class_<inlong::InLongApi>(m, "InLongApi")
        .def(py::init<>())
        .def("init_api", [](inlong::InLongApi& self, const char* config_path) {
            stop_callbacks = false;
            g_py_callbacks.clear();
            py::gil_scoped_release release;
            int result = self.InitApi(config_path);
            return result;
        })
        .def("send", [](inlong::InLongApi& self, const char* groupId, const char* streamId, const char* msg, int32_t msgLen, py::object pyCallback = py::none()) {
            // Recalculate the message length by C++ rules
            int32_t sendLen = static_cast<int32_t>(strlen(msg));

            if (!pyCallback.is(py::none())) {
                g_py_callbacks[UserCallBackBridge] = pyCallback.cast<py::function>();
                py::gil_scoped_release release;
                int result = self.Send(groupId, streamId, msg, sendLen, UserCallBackBridge);
                return result;
            } else {
                int result = self.Send(groupId, streamId, msg, sendLen, nullptr);
                return result;
            }
        })
        .def("close_api", [](inlong::InLongApi& self, int32_t timeout_ms) {
            py::gil_scoped_release release;
            int result = self.CloseApi(timeout_ms);
            stop_callbacks = true;
            std::unique_lock<std::mutex> lock(callback_mutex);
            py::gil_scoped_acquire acquire;
            return result;
        });
}