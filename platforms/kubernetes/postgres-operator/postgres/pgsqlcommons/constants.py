# constants
'''
constants
'''
VIP = "vip"
RADONDB_POSTGRES = "radondb-postgres"
POSTGRES_OPERATOR = "postgres-operator"
AUTOFAILOVER = "autofailover"
POSTGRESQL = "postgresql"
DISASTER = "disaster"
AUTOCTL_DISASTER_NAME = "pgdisaster"
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
SPEC_POSTGRESQL_READWRITE_RESOURCES_LIMITS_CPU = "cpu"
SPEC_POSTGRESQL_READWRITE_RESOURCES_LIMITS_MEMORY = "memory"
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
SPEC_DELETE_S3 = "deletes3"
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
SPEC_BACKUPCLUSTER = "backupCluster"
SPEC_BACKUPTOS3 = "backupToS3"
SPEC_BACKUPTOS3_NAME = "name"
SPEC_BACKUPTOS3_MANUAL = "manual"
SPEC_BACKUPTOS3_MANUAL_TRIGGER_ID = "trigger-id"
SPEC_BACKUPTOS3_CRON = "cron"
SPEC_BACKUPTOS3_CRON_ENABLE = "enable"
SPEC_BACKUPTOS3_CRON_SCHEDULE = "schedule"
SPEC_BACKUPTOS3_POLICY = "policy"
SPEC_BACKUPTOS3_POLICY_ARCHIVE = "archive"
SPEC_BACKUPTOS3_POLICY_ARCHIVE_DEFAULT_VALUE = "off"
SPEC_BACKUPTOS3_POLICY_COMPRESSION = "compression"
SPEC_BACKUPTOS3_POLICY_COMPRESSION_DEFAULT_VALUE = "none"
SPEC_BACKUPTOS3_POLICY_ENCRYPTION = "encryption"
SPEC_BACKUPTOS3_POLICY_ENCRYPTION_DEFAULT_VALUE = "none"
SPEC_BACKUPTOS3_POLICY_RETENTION = "retention"
SPEC_BACKUPTOS3_POLICY_RETENTION_DEFAULT_VALUE = "none"
SPEC_BACKUPTOS3_POLICY_RETENTION_DELETE_ALL_VALUE = "delete_all"
RESTORE = "restore"
RESTORE_FROMSSH = "fromssh"
RESTORE_FROMSSH_PATH = "path"
RESTORE_FROMSSH_ADDRESS = "address"
RESTORE_FROMSSH_LOCAL = "local"
RESTORE_FROMS3 = "froms3"
RESTORE_FROMS3_NAME = "name"
RESTORE_FROMS3_RECOVERY = "recovery"
RESTORE_FROMS3_RECOVERY_LATEST = "latest"
RESTORE_FROMS3_RECOVERY_LATEST_FULL = "latest-full"
RESTORE_FROMS3_RECOVERY_OLDEST_FULL = "oldest-full"
SPEC_VOLUME_TYPE = "volume_type"
SPEC_VOLUME_LOCAL = "local"
SPEC_VOLUME_CLOUD = "cloud"
SPEC_REBUILD = "rebuild"
SPCE_REBUILD_NODENAMES = "nodeName"
SPEC_SWITCHOVER = "switchover"
SPEC_SWITCHOVER_MASTERNODE = "masterNode"
SPEC_DISASTERBACKUP = "disasterBackup"
SPEC_DISASTERBACKUP_ENABLE = "enable"
SPEC_DISASTERBACKUP_STREAMING = "streaming"
SPEC_DISASTERBACKUP_AUTOCTL_NODE = "autoctl_node"
SPEC_DISASTERBACKUP_PGAUTOFAILOVER_REPLICATOR = "pgautofailover_replicator"
SPEC_DISASTERBACKUP_MONITOR_HOSTNAME = "monitor_hostname"

# api
API_GROUP = "postgres.radondb.io"
API_VERSION_V1 = "v1"
RESOURCE_POSTGRESQL = "postgresqls"
RESOURCE_KIND_POSTGRESQL = "PostgreSQL"
RESOURCE_POSTGRESQL_BACKUP = "postgresqlbackups"
RESOURCE_KIND_POSTGRESQLBACKUP = "PostgreSQLBackup"

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
CLUSTER_STATUS_BACKUP = "backups_list"
CLUSTER_STATUS_ARCHIVE = "backups_wal_archive"
CLUSTER_STATUS_SERVER_CRT = "server_crt"
CLUSTER_STATUS_CRON_NEXT_RUN = "cron_next_run_time"
CLUSTER_STATUS_DISASTER_BACKUP_STATUS = 'disaster_backup_status'
CLUSTER_STATUS_TIMER = 'timer'

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
LABEL_ROLE_UNKNOWN = "unknown"

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

# S3
SPEC_S3 = "S3"
SPEC_S3_ACCESS_KEY = "ACCESS_KEY"
SPEC_S3_SECRET_KEY = "SECRET_KEY"
SPEC_S3_ENDPOINT = "ENDPOINT"
SPEC_S3_BUCKET = "BUCKET"
SPEC_S3_PATH = "PATH"

# storage
STORAGE_CLASS_NAME = "storageClassName"

# pod PriorityClass
SPEC_POD_PRIORITY_CLASS = "priorityClassName"
SPEC_POD_PRIORITY_CLASS_SCOPE_NODE = "system-node-critical"
SPEC_POD_PRIORITY_CLASS_SCOPE_CLUSTER = "system-cluster-critical"

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
PG_DATABASE_RESTORING_DIR = "/var/lib/postgresql/data/pg_data_restoring"
INIT_FINISH = "init_finish"
RECOVERY_FINISH = "recovery_finish"
PG_LOG_FILENAME = "start.log"

PG_CONFIG_PREFIX = "PG_CONFIG_"
PG_HBA_PREFIX = "PG_HBA_"

DIR_ASSIST = "assist"
DIR_AUTO_FAILOVER = "auto_failover"
DIR_BACKUP = "backup"
DIR_BARMAN = "barman"

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
virtual_server fwmark 1 {{
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

# config

PGLOG_DIR = "log"
PRIMARY_FORMATION = " --formation primary "
FIELD_DELIMITER = "-"
WAITING_POSTGRESQL_READY_COMMAND = ["pgtools", "-a"]
POD_READY_COMMAND = ["echo"]
INIT_FINISH_MESSAGE = "init postgresql finish"
STOP_FAILED_MESSAGE = "stop auto_failover failed"
POSTGRESQL_NOT_RUNNING_MESSAGE = "can't connect database."
SWITCHOVER_FAILED_MESSAGE = "switchover failed"
AUTO_FAILOVER_PORT = 55555
EXPORTER_PORT = 9187
DIFF_ADD = "add"
DIFF_CHANGE = "change"
DIFF_REMOVE = "remove"
PGPASSFILE_PATH = ASSIST_DIR + "/pgpassfile"
DIFF_FIELD_DISASTERBACKUP = (SPEC, SPEC_DISASTERBACKUP)
DIFF_FIELD_ACTION = (SPEC, ACTION)
DIFF_FIELD_SERVICE = (SPEC, SERVICES)
DIFF_FIELD_AUTOFAILOVER_HBAS = (SPEC, AUTOFAILOVER, HBAS)
DIFF_FIELD_POSTGRESQL_HBAS = (SPEC, POSTGRESQL, HBAS)
DIFF_FIELD_AUTOFAILOVER_CONFIGS = (SPEC, AUTOFAILOVER, CONFIGS)
DIFF_FIELD_POSTGRESQL_CONFIGS = (SPEC, POSTGRESQL, CONFIGS)
DIFF_FIELD_POSTGRESQL_USERS = (SPEC, POSTGRESQL, SPEC_POSTGRESQL_USERS)
DIFF_FIELD_POSTGRESQL_USERS_ADMIN = (SPEC, POSTGRESQL, SPEC_POSTGRESQL_USERS,
                                     SPEC_POSTGRESQL_USERS_ADMIN)
DIFF_FIELD_POSTGRESQL_USERS_MAINTENANCE = (SPEC, POSTGRESQL,
                                           SPEC_POSTGRESQL_USERS,
                                           SPEC_POSTGRESQL_USERS_MAINTENANCE)
DIFF_FIELD_POSTGRESQL_USERS_NORMAL = (SPEC, POSTGRESQL, SPEC_POSTGRESQL_USERS,
                                      SPEC_POSTGRESQL_USERS_NORMAL)
DIFF_FIELD_STREAMING = (SPEC, POSTGRESQL, READONLYINSTANCE, STREAMING)
DIFF_FIELD_READWRITE_REPLICAS = (SPEC, POSTGRESQL, READWRITEINSTANCE, REPLICAS)
DIFF_FIELD_READWRITE_MACHINES = (SPEC, POSTGRESQL, READWRITEINSTANCE, MACHINES)
DIFF_FIELD_READONLY_REPLICAS = (SPEC, POSTGRESQL, READONLYINSTANCE, REPLICAS)
DIFF_FIELD_READONLY_MACHINES = (SPEC, POSTGRESQL, READONLYINSTANCE, MACHINES)
DIFF_FIELD_AUTOFAILOVER_PODSPEC = (SPEC, AUTOFAILOVER, PODSPEC)
DIFF_FIELD_READWRITE_PODSPEC = (SPEC, POSTGRESQL, READWRITEINSTANCE, PODSPEC)
DIFF_FIELD_READONLY_PODSPEC = (SPEC, POSTGRESQL, READONLYINSTANCE, PODSPEC)
DIFF_FIELD_AUTOFAILOVER_VOLUME = (SPEC, AUTOFAILOVER, VOLUMECLAIMTEMPLATES)
DIFF_FIELD_READWRITE_VOLUME = (SPEC, POSTGRESQL, READWRITEINSTANCE,
                               VOLUMECLAIMTEMPLATES)
DIFF_FIELD_READONLY_VOLUME = (SPEC, POSTGRESQL, READONLYINSTANCE,
                              VOLUMECLAIMTEMPLATES)
DIFF_FIELD_SPEC_ANTIAFFINITY = (SPEC, SPEC_ANTIAFFINITY)
DIFF_FIELD_SPEC_BACKUPCLUSTER = (SPEC, SPEC_BACKUPCLUSTER)
DIFF_FIELD_SPEC_BACKUPS3_MANUAL = (SPEC, SPEC_BACKUPCLUSTER, SPEC_BACKUPTOS3,
                                   SPEC_BACKUPTOS3_MANUAL)
DIFF_FIELD_SPEC_REBUILD = (SPEC, SPEC_REBUILD)
DIFF_FIELD_SPEC_REBUILD_NODENAMES = (SPEC, SPEC_REBUILD,
                                     SPCE_REBUILD_NODENAMES)
DIFF_FIELD_SPEC_SWITCHOVER = (SPEC, SPEC_SWITCHOVER)
DIFF_FIELD_SPEC_SWITCHOVER_MASTERNODE = (SPEC, SPEC_SWITCHOVER,
                                         SPEC_SWITCHOVER_MASTERNODE)

STATEFULSET_REPLICAS = 1
PG_CONFIG_MASTER_LARGE_THAN_SLAVE = ("max_connections", "max_worker_processes",
                                     "max_wal_senders",
                                     "max_prepared_transactions",
                                     "max_locks_per_transaction")
PG_CONFIG_IGNORE = ("block_size", "data_checksums", "data_directory_mode",
                    "debug_assertions", "integer_datetimes", "lc_collate",
                    "lc_ctype", "max_function_args", "max_identifier_length",
                    "max_index_keys", "segment_size", "server_encoding",
                    "server_version", "server_version_num", "ssl_library",
                    "wal_block_size", "wal_segment_size")
PG_CONFIG_RESTART = (
    "allow_system_table_mods", "archive_mode", "autovacuum_freeze_max_age",
    "autovacuum_max_workers", "autovacuum_multixact_freeze_max_age", "bonjour",
    "bonjour_name", "cluster_name", "config_file", "data_directory",
    "data_sync_retry", "dynamic_shared_memory_type", "event_source",
    "external_pid_file", "hba_file", "hot_standby", "huge_pages",
    "huge_page_size", "ident_file", "ignore_invalid_pages", "jit_provider",
    "listen_addresses", "logging_collector", "max_connections",
    "max_files_per_process", "max_locks_per_transaction",
    "max_logical_replication_workers", "max_pred_locks_per_transaction",
    "max_prepared_transactions", "max_replication_slots", "max_wal_senders",
    "max_worker_processes", "min_dynamic_shared_memory",
    "old_snapshot_threshold", "pg_stat_statements.max", "port",
    "primary_conninfo", "primary_slot_name", "recovery_target",
    "recovery_target_action", "recovery_target_inclusive",
    "recovery_target_lsn", "recovery_target_name", "recovery_target_time",
    "recovery_target_timeline", "recovery_target_xid", "restore_command",
    "shared_buffers", "shared_memory_type", "shared_preload_libraries",
    "superuser_reserved_connections", "track_activity_query_size",
    "track_commit_timestamp", "unix_socket_directories", "unix_socket_group",
    "unix_socket_permissions", "wal_buffers", "wal_level", "wal_log_hints")
units = {
    "Ki": 1 << 10,
    "Mi": 1 << 20,
    "Gi": 1 << 30,
    "Ti": 1 << 40,
    "Pi": 1 << 50,
    "Ei": 1 << 60,
    "K": pow(1000, 1),
    "M": pow(1000, 2),
    "G": pow(1000, 3),
    "T": pow(1000, 4),
    "P": pow(1000, 5),
    "E": pow(1000, 6)
}

POSTGRESQL_PAUSE = "pause"
POSTGRESQL_RESUME = "resume"
KEEPALIVED_CONF = "/etc/keepalived/keepalived.conf"
START_KEEPALIVED = "systemctl restart keepalived.service"
STOP_KEEPALIVED = "systemctl stop keepalived.service"
STATUS_KEEPALIVED = "systemctl status keepalived.service"
RECOVERY_CONF_FILE = "postgresql-auto-failover-standby.conf"
RECOVERY_SET_FILE = "postgresql-auto-failover.conf"
STANDBY_SIGNAL = "standby.signal"
RECOVERY_SIGNAL = "recovery.signal"
POSTMASTER_FILE = "postmaster.pid"
POSTGRESQL_BACKUP_RESTORE_CONFIG = "postgresql_backup_restore.conf"
GET_INET_CMD = "ip addr | grep inet"
SUCCESS_CHECKPOINT = "CHECKPOINT"
CONTAINER_ENV = "env"
CONTAINER_ENV_NAME = "name"
CONTAINER_ENV_VALUE = "value"
EXPORTER_CONTAINER_INDEX = 1
POSTGRESQL_CONTAINER_INDEX = 0
NODE_PRIORITY_HIGH = 100
NODE_PRIORITY_DEFAULT = 50
NODE_PRIORITY_NEVER = 0
WAIT_TIMEOUT = MINUTES * 20
POSTGRESQL_IMAGE_VERSION_v1_1_0 = 'v1.1.0'

## backup
BACKUP_MODE_NONE = "none"
BACKUP_MODE_S3_MANUAL = "manual"
BACKUP_MODE_S3_CRON = "cron"
BACKUP_NAME = "BACKUP_NAME"
RESTORE_NAME = "RESTORE_NAME"

SPECIAL_CHARACTERS = "/##/"

# ### messages
FILE_NOT_EXISTS = "No_such_file"
