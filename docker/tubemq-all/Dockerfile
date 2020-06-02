# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# tubemq depend on zookeeper
FROM zookeeper:3.4
# install tools for debug
RUN apt-get update \
    && apt-get install iputils-ping curl procps -y
# add tarball from target output
ARG TUBEMQ_TARBALL=apache-tubemq-server-0.5.0-incubating-SNAPSHOT-bin.tar.gz
ADD ${TUBEMQ_TARBALL} /
RUN mv /apache-tubemq-server-* /opt/tubemq-server
# overwrite default jvm size
ENV MASTER_JVM_SIZE="-Xmx1g -Xms1g" BROKER_JVM_SIZE="-Xmx1g -Xms1g"
ADD tubemq.sh /opt/tubemq-server/bin/
RUN chmod +x /opt/tubemq-server/bin/*
# standalone default
ENV TARGET=standalone
WORKDIR /opt/tubemq-server/
# master port
EXPOSE 8000 8080 9001
# broker port
EXPOSE 8123 8081
CMD ["/opt/tubemq-server/bin/tubemq.sh"]