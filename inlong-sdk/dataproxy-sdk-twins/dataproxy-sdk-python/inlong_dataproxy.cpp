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
#include <pybind11/stl.h>
#include <inlong_api.h>

namespace py = pybind11;
using namespace inlong;

class PyInLongApi : public InLongApi {
public:
    int32_t PySend(const char *inlong_group_id, const char *inlong_stream_id, const char *msg, int32_t msg_len, py::function callback_func) {
        if (callback_func) {
            py_callback = callback_func;
            return Send(inlong_group_id, inlong_stream_id, msg, msg_len, &PyInLongApi::CallbackFunc);
        } else {
            return Send(inlong_group_id, inlong_stream_id, msg, msg_len, nullptr);
        }
    }

private:
    static py::function py_callback;

    static int CallbackFunc(const char *a, const char *b, const char *c, int32_t d, const int64_t e, const char *f) {
        return py_callback(a, b, c, d, e, f).cast<int>();
    }
};

py::function PyInLongApi::py_callback;

PYBIND11_MODULE(inlong_dataproxy, m) {
    m.doc() = "This module provides InLong dataproxy api to send message to InLong dataproxy.";

    py::class_<PyInLongApi>(m, "InLongApi")
        .def(py::init<>())
        .def("init_api", &PyInLongApi::InitApi, py::arg("config_path"))
        .def("add_bid", &PyInLongApi::AddBid, py::arg("group_ids"))
        .def("send", &PyInLongApi::PySend, py::arg("inlong_group_id"), py::arg("inlong_stream_id"), py::arg("msg"), py::arg("msg_len"), py::arg("callback_func") = nullptr)
        .def("close_api", &PyInLongApi::CloseApi, py::arg("max_waitms"));
}
