/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.pojo.user;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response body returned to the dashboard after a successful login.
 *
 * <p>To enforce a password reset when a user logs in with the default credentials,
 * the {@link #mustChangePassword} field is set to {@code true} when: the username
 * matches the configured default admin account (defaults to {@code admin}), and
 * the password hash stored in the database is still SHA-256("inlong"). The frontend
 * uses this flag to display a mandatory password-change dialog.
 *
 * <p>This DTO is the new response structure for {@code AnnoController#login}.
 * Legacy frontend clients simply check {@code data === true}; the new JSON object
 * is still truthy (non-null), so backward compatibility is preserved. Older
 * dashboard versions merely see a few extra unused fields.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Login response with hints for the dashboard")
public class LoginResponse {

    @ApiModelProperty(value = "Always true when this DTO is returned (login succeeded).")
    private Boolean success;

    @ApiModelProperty(value = "Logged-in username, echoed back for convenience.")
    private String username;

    @ApiModelProperty(value = "Database id of the logged-in user (needed by the change-password endpoint).")
    private Integer userId;

    @ApiModelProperty(value = "True when the user must rotate their password before doing anything else.")
    private Boolean mustChangePassword;
}
