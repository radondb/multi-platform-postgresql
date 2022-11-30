# constants
'''
constants
'''
VIP = "vip"
RADONDB_POSTGRES = "radondb-postgres"
POSTGRES_OPERATOR = "postgres-operator"
AUTOFAILOVER = "autofailover"
POSTGRESQL = "postgresql"
READWRITEINSTANCE = "readwriteinstance"
READONLYINSTANCE = "readonlyinstance"
MACHINES = "machines"
ACTION = "action"
ACTION_START = "start"
ACTION_STOP = "stop"
IMAGE = "image"
PODSPEC = "podspec"
SPEC = "spec"
CONTAINERS = "containers"
CONTAINER_NAME = "name"
PODSPEC_CONTAINERS_POSTGRESQL_CONTAINER = "postgresql"
PODSPEC_CONTAINERS_EXPORTER_CONTAINER = "exporter"
PRIME_SERVICE_PORT_NAME = "prime"
EXPORTER_SERVICE_PORT_NAME = "exporter"
HBAS = "hbas"
CONFIGS = "configs"
REPLICAS = "replicas"
VOLUMECLAIMTEMPLATES = "volumeClaimTemplates"
AUTOCTL_NODE = "autoctl_node"
PGAUTOFAILOVER_REPLICATOR = "pgautofailover_replicator"
STREAMING = "streaming"
STREAMING_ASYNC = "async"
STREAMING_SYNC = "sync"
DELETE_PVC = "deletepvc"
UPDATE_TOLERATION = "updatetoleration"
POSTGRESQL_PVC_NAME = "data"
SUCCESS = "exec_success"
FAILED = "exec_failed"
SERVICES = "services"
SELECTOR = "selector"
SERVICE_AUTOFAILOVER = "autofailover"
SERVICE_PRIMARY = "primary"
SERVICE_STANDBY = "standby"
SERVICE_READONLY = "readonly"
SERVICE_STANDBY_READONLY = "standby-readonly"
SPEC_POSTGRESQL_USERS = "users"
SPEC_POSTGRESQL_USERS_ADMIN = "admin"
SPEC_POSTGRESQL_USERS_MAINTENANCE = "maintenance"
SPEC_POSTGRESQL_USERS_NORMAL = "normal"
SPEC_POSTGRESQL_USERS_USER_PASSWORD = "password"
SPEC_POSTGRESQL_USERS_USER_NAME = "name"
RESTORE = "restore"
RESTORE_FROMSSH = "fromssh"
RESTORE_FROMSSH_PATH = "path"
RESTORE_FROMSSH_ADDRESS = "address"
RESTORE_FROMSSH_LOCAL = "local"
SPEC_VOLUME_TYPE = "volume_type"
SPEC_VOLUME_LOCAL = "local"
SPEC_VOLUME_CLOUD = "cloud"

# api
API_GROUP = "postgres.radondb.io"
API_VERSION_V1 = "v1"
RESOURCE_POSTGRESQL = "postgresqls"
RESOURCE_KIND_POSTGRESQL = "PostgreSQL"

# status.CLUSTER_CREATE_CLUSTER
CLUSTER_STATE = "state"
CLUSTER_CREATE_BEGIN = "begin"
CLUSTER_CREATE_ADD_FAILOVER = "addition failover"
CLUSTER_CREATE_ADD_READWRITE = "addition readwrite"
CLUSTER_CREATE_ADD_READONLY = "addition readonly"
CLUSTER_CREATE_FINISH = "finish"

CLUSTER_STATUS = "status"
CLUSTER_STATUS_CREATE = "Creating"
CLUSTER_STATUS_UPDATE = "Updating"
CLUSTER_STATUS_RUN = "Running"
CLUSTER_STATUS_CREATE_FAILED = "CreateFailed"
CLUSTER_STATUS_UPDATE_FAILED = "UpdateFailed"
CLUSTER_STATUS_TERMINATE = "Terminating"
CLUSTER_STATUS_STOP = "stop"

# base label
BASE_LABEL_PART_OF = "part-of"
BASE_LABEL_MANAGED_BY = "managed-by"
BASE_LABEL_NAME = "app-name"
BASE_LABEL_NAMESPACE = "app-namespace"

# label node
LABEL_NODE = "node-name"
LABEL_NODE_AUTOFAILOVER = "autofailover"
LABEL_NODE_POSTGRESQL = "postgresql"
LABEL_NODE_USER_SERVICES = "user-services"
LABEL_NODE_STATEFULSET_SERVICES = "statefulset-services"

# label sub node
LABEL_SUBNODE = "subnode-name"
LABEL_SUBNODE_READWRITE = "readwrite"
LABEL_SUBNODE_AUTOFAILOVER = "autofailover"
LABEL_SUBNODE_READONLY = "readonly"

# labe role
LABEL_ROLE = "role"
LABEL_ROLE_PRIMARY = "primary"
LABEL_ROLE_STANDBY = "standby"

# other label
LABEL_STATEFULSET_NAME = "statefulset"

# other
MACHINE_MODE = "machine"
K8S_MODE = "k8s"
PGHOME = "/var/lib/postgresql"

# antiaffinity
SPEC_ANTIAFFINITY = "antiaffinity"
SPEC_ANTIAFFINITY_POLICY = "policy"
SPEC_ANTIAFFINITY_REQUIRED = "required"
SPEC_ANTIAFFINITY_PREFERRED = "preferred"
SPEC_ANTIAFFINITY_POLICY_REQUIRED = "requiredDuringSchedulingIgnoredDuringExecution"
SPEC_ANTIAFFINITY_POLICY_PREFERRED = "preferredDuringSchedulingIgnoredDuringExecution"
SPEC_ANTIAFFINITY_PODANTIAFFINITYTERM = "podAntiAffinityTerm"
SPEC_ANTIAFFINITY_TOPOLOGYKEY = "topologyKey"

# time
SECONDS = 1
MINUTES = SECONDS * 60
HOURS = MINUTES * 60
DAYS = HOURS * 24

# docker-compose
DOCKER_COMPOSE_FILE = "docker-compose.yaml"
DOCKER_COMPOSE_FILE_DATA = '''
version: '3.1'
services:
  %s:
    container_name: %s
    image: ${image}
    network_mode: host
    restart: always
    volumes:
      - ${pgdata}:/var/lib/postgresql/data
      - /dev/shm:/dev/shm
    env_file:
      - ./pgenv
    command:
      - auto_failover
  %s:
    container_name: %s
    image: ${exporterimage}
    network_mode: host
    restart: always
    env_file:
      - ./exporterenv
'''

# .env
DOCKER_COMPOSE_ENV = ".env"
DOCKER_COMPOSE_ENV_DATA = '''
image={0}
host_name={1}
pgdata={2}
exporterimage={3}
'''

# env_file
DOCKER_COMPOSE_ENVFILE = "pgenv"
DOCKER_COMPOSE_EXPORTER_ENVFILE = "exporterenv"

# docker-compose data dirctory
DOCKER_COMPOSE_DIR = "docker_compose"
PGDATA_DIR = "pgdata"
ASSIST_DIR = "/var/lib/postgresql/data/assist"
DATA_DIR = "/var/lib/postgresql/data"
PG_DATABASE_DIR = "/var/lib/postgresql/data/pg_data"
INIT_FINISH = "init_finish"

PG_CONFIG_PREFIX = "PG_CONFIG_"
PG_HBA_PREFIX = "PG_HBA_"

# net
# main_vip
# read_vip
# port
# real_main_servers
# real_read_servers
LVS_BODY = '''
vrrp_instance VI_1 {{
    state BACKUP
    nopreempt
    interface {net}
    virtual_router_id {routeid}
    priority 100

    authentication {{
        auth_type PASS
        auth_pass pass
    }}
    virtual_ipaddress {{
        {main_vip}
        {read_vip}
    }}
}}

virtual_server {main_vip} {port} {{
    delay_loop 10
    lb_algo lc
    lb_kind DR
    protocol TCP

    {real_main_servers}
}}
virtual_server {read_vip} {port} {{
    delay_loop 10
    lb_algo lc
    lb_kind DR
    protocol TCP

    {real_read_servers}
}}
'''

# ip
# port
LVS_REAL_MAIN_SERVER = '''
real_server {ip} {port} {{
    weight 1
    MISC_CHECK {{
        misc_path "/usr/local/bin/pgtools --isprimary {ip}"
        misc_timeout 60
    }}
}}
'''

# ip
# port
LVS_REAL_READ_SERVER = '''
real_server {ip} {port} {{
    weight 1
    MISC_CHECK {{
        misc_path "/usr/local/bin/pgtools --isstandby {ip}"
        misc_timeout 60
    }}
}}
'''

LVS_REAL_EMPTY_SERVER = '''
real_server {ip} {port} {{
    weight 1
    MISC_CHECK {{
        misc_path "ls /tmp/file_not_exists_pg"
        misc_timeout 60
    }}
}}
'''

# main_vip
# read_vip
LVS_SET_NET = '''
/sbin/ifconfig lo down;
/sbin/ifconfig lo up;
echo 1 > /proc/sys/net/ipv4/conf/lo/arp_ignore;
echo 2 > /proc/sys/net/ipv4/conf/lo/arp_announce;
echo 1 > /proc/sys/net/ipv4/conf/all/arp_ignore;
echo 2 > /proc/sys/net/ipv4/conf/all/arp_announce;
/sbin/ifconfig lo:0 {main_vip} broadcast {main_vip} netmask 255.255.255.255 up;
/sbin/route add -host {main_vip} dev lo:0;
/sbin/ifconfig lo:1 {read_vip} broadcast {read_vip} netmask 255.255.255.255 up;
/sbin/route add -host {read_vip} dev lo:1;
'''

# LO net
LVS_UNSET_NET = '''
/sbin/ifconfig lo:0 down;
/sbin/ifconfig lo:1 down;
echo 0 > /proc/sys/net/ipv4/conf/lo/arp_ignore;
echo 0 > /proc/sys/net/ipv4/conf/lo/arp_announce;
echo 0 > /proc/sys/net/ipv4/conf/all/arp_ignore;
echo 0 > /proc/sys/net/ipv4/conf/all/arp_announce;
'''
