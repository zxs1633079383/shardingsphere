#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM alpine AS prepare

ARG APP_NAME
ENV LOCAL_PATH=/opt/shardingsphere-proxy

ADD target/${APP_NAME}.tar.gz /opt
RUN mv /opt/${APP_NAME} ${LOCAL_PATH} && mkdir -p ${LOCAL_PATH}/ext-lib

FROM eclipse-temurin:25-jdk@sha256:572fe7b5b3ca8beb3b3aca96a7a88f1f7bc98a3bdffd03784a4568962c1a963a
LABEL org.opencontainers.image.authors="ShardingSphere dev@shardingsphere.apache.org"

ENV LOCAL_PATH=/opt/shardingsphere-proxy
ENV IS_DOCKER=true

COPY --from=prepare ${LOCAL_PATH} ${LOCAL_PATH}
COPY ./ext-lib/postgresql-42.7.8.jar /opt/shardingsphere-proxy/ext-lib/
COPY ./ext-lib/xpa-sharding-algorithm-1.0-SNAPSHOT.jar /opt/shardingsphere-proxy/lib/
COPY ./ext-lib/xpa-sharding-algorithm-1.0-SNAPSHOT.jar /opt/shardingsphere-proxy/ext-lib/
ENTRYPOINT ${LOCAL_PATH}/bin/start.sh ${PORT}
