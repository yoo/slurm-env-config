package main

import (
	"io"
	"log"
	"os"
	"reflect"
	"unicode"
)

type Controller struct {
	ControlMachine   string
	ControlAddr      string
	BackupController string
	BackupAddr       string
}

type General struct {
	AuthType                       string
	AuthInfo                       string
	CacheGroups                    string
	CheckpointType                 string
	CryptoType                     string
	CoreSpecPlugin                 string
	DisableRootJobs                string
	EnforcePartLimits              string
	Epilog                         string
	EpilogSlurmctld                string
	FirstJobId                     string
	MaxJobId                       string
	GresTypes                      string
	GroupUpdateForce               string
	GroupUpdateTime                string
	JobCheckpointDir               string
	JobCredentialPrivateKey        string
	JobCredentialPublicCertificate string
	JobFileAppend                  string
	JobRequeue                     string
	JobSubmitPlugins               string
	KillOnBadExit                  string
	Licenses                       string
	MailProg                       string
	MaxJobCount                    string
	MaxStepCount                   string
	MaxTasksPerNode                string
	MpiDefault                     string
	MpiParams                      string
	PluginDir                      string
	PlugStackConfig                string
	PrivateData                    string
	ProctrackType                  string
	Prolog                         string
	PrologSlurmctld                string
	PropagatePrioProcess           string
	PropagateResourceLimits        string
	PropagateResourceLimitsExcept  string
	ReturnToService                string
	SallocDefaultCommand           string
	SlurmctldPidFile               string
	SlurmctldPort                  string
	SlurmdPidFile                  string
	SlurmdPort                     string
	SlurmdSpoolDir                 string
	SlurmUser                      string
	SlurmdUser                     string
	SrunEpilog                     string
	SrunProlog                     string
	StateSaveLocation              string
	SwitchType                     string
	TaskEpilog                     string
	TaskPlugin                     string
	TaskPluginParam                string
	TaskProlog                     string
	TopologyPlugin                 string
	TmpFS                          string
	TrackWCKey                     string
	TreeWidth                      string
	UnkillableStepProgram          string
	UsePAM                         string
}

type Timers struct {
	BatchStartTimeout     string
	CompleteWait          string
	EpilogMsgTime         string
	GetEnvTimeout         string
	HealthCheckInterval   string
	HealthCheckProgram    string
	InactiveLimit         string
	KillWait              string
	MessageTimeout        string
	ResvOverRun           string
	MinJobAge             string
	OverTimeLimit         string
	SlurmctldTimeout      string
	SlurmdTimeout         string
	UnkillableStepTimeout string
	VSizeFactor           string
	Waittime              string
}

type Scheduling struct {
	DefMemPerCPU         string
	FastSchedule         string
	MaxMemPerCPU         string
	SchedulerRootFilter  string
	SchedulerTimeSlice   string
	SchedulerType        string
	SchedulerPort        string
	SelectType           string
	SelectTypeParameters string
}

type JobPriority struct {
	PriorityType             string
	PriorityDecayHalfLife    string
	PriorityCalcPeriod       string
	PriorityFavorSmall       string
	PriorityMaxAge           string
	PriorityUsageResetPeriod string
	PriorityWeightAge        string
	PriorityWeightFairshare  string
	PriorityWeightJobSize    string
	PriorityWeightPartition  string
	PriorityWeightQOS        string
}

type LoggingAccounting struct {
	AccountingStorageEnforce    string
	AccountingStorageHost       string
	AccountingStorageBackupHost string
	AccountingStorageLoc        string
	AccountingStoragePass       string
	AccountingStoragePort       string
	AccountingStorageType       string
	AccountingStorageUser       string
	AccountingStoreJobComment   string
	AcctGatherNodeFreq          string
	AcctGatherInfinibandType    string
	AcctGatherFilesystemType    string
	AcctGatherProfileType       string
	ClusterName                 string
	ChosLoc                     string
	DebugFlags                  string
	JobCompHost                 string
	JobCompLoc                  string
	JobCompPass                 string
	JobCompPort                 string
	JobCompType                 string
	JobCompUser                 string
	JobAcctGatherFrequency      string
	JobAcctGatherType           string
	SlurmctldDebug              string
	SlurmctldLogFile            string
	SlurmdDebug                 string
	SlurmdLogFile               string
	SlurmSchedLogFile           string
	SlurmSchedLogLevel          string
}

type PowerSave struct {
	SuspendProgram  string
	ResumeProgram   string
	SuspendTimeout  string
	ResumeTimeout   string
	ResumeRate      string
	SuspendExcNodes string
	SuspendExcParts string
	SuspendRate     string
	SuspendTime     string
}

type Node struct {
	NodeName        string
	NodeHostname    string
	NodeAddr        string
	Boards          string
	CoreSpecCount   string
	CoresPerSocket  string
	CPUs            string
	CPUSpecList     string
	Feature         string
	Gres            string
	MemSpecLimit    string
	Port            string
	Procs           string
	RealMemory      string
	Reason          string
	Sockets         string
	SocketsPerBoard string
	State           string
	ThreadsPerCore  string
	TmpDisk         string
	Weight          string
	DownNodes       string
}

type Partition struct {
	PartitionName        string
	AllocNodes           string
	AllowAccounts        string
	AllowGroups          string
	AllowQos             string
	Alternate            string
	Default              string
	DefMemPerCPU         string
	DefMemPerNode        string
	DenyAccounts         string
	DenyQos              string
	DefaultTime          string
	DisableRootJobs      string
	GraceTime            string
	Hidden               string
	LLN                  string
	MaxCPUsPerNode       string
	MaxMemPerCPU         string
	MaxMemPerNode        string
	MaxNodes             string
	MaxTime              string
	MinNodes             string
	Nodes                string
	PreemptMode          string
	Priority             string
	ReqResv              string
	RootOnly             string
	SelectTypeParameters string
	Shared               string
	State                string
}

type Frontend struct {
	AllowGroups  string
	AllowUsers   string
	DenyGroups   string
	DenyUsers    string
	FrontendName string
	FrontendAddr string
	Port         string
	Reason       string
	State        string
}

type Config struct {
	Controller        Controller
	General           General
	Timers            Timers
	Scheduling        Scheduling
	JobPriority       JobPriority
	LoggingAccounting LoggingAccounting
	PowerSave         PowerSave
	Node              []Node
	Partition         []Partition
	Frontend          []Frontend
}

func main() {
	var out io.Writer
	if len(os.Args) > 1 {
		fi, err := os.Create(os.Args[1])
		if err != nil {
			log.Println(err)
		}
		out = fi
	} else {
		out = os.Stdout
	}
	conf := Config{}
	conf.FromEvn()
	conf.Write(out)
}

func (c *Config) Write(w io.Writer) {

	w.Write([]byte("#slurm.conf\n\n"))
	w.Write([]byte("#\n# Controll Machines\n#\n"))
	writeStructToConfig(c.Controller, w)
}

func writeStructToConfig(st interface{}, w io.Writer) {

	dlim := []byte{'='}
	endl := []byte{'\n'}

	val := reflect.ValueOf(st)
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		configField := typ.Field(i)
		configValue := val.Field(i)
		s := configValue.String()
		if s == "" {
			continue
		}

		w.Write([]byte(configField.Name))
		w.Write(dlim)
		w.Write([]byte(s))
		w.Write(endl)
	}

}

func (c *Config) FromEvn() {
	val := reflect.ValueOf(c).Elem()
	for i := 0; i < val.NumField(); i++ {
		confVal := val.Field(i)
		switch confVal.Kind() {
		case reflect.Slice:
			for i := 0; i < confVal.Len(); i++ {
				fillStructFromEnv(confVal.Index(i))
			}
		case reflect.Struct:
			fillStructFromEnv(confVal)
		}
	}
}

func fillStructFromEnv(val reflect.Value) {
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		for i := 0; i < val.NumField(); i++ {
			configField := typ.Field(i)
			configVal := val.Field(i)

			envName := toEnvName(configField.Name)
			configVal.SetString(os.Getenv(envName))
		}
	}
}

func toEnvName(name string) string {
	envName := []rune("SLURM")
	for _, r := range name {
		if unicode.IsUpper(r) {
			envName = append(envName, '_', r)
		} else {
			envName = append(envName, unicode.ToUpper(r))
		}
	}
	return string(envName)
}
