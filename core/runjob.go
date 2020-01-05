package core

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fsouza/go-dockerclient"
	"github.com/gobs/args"
)

var dockercfg *docker.AuthConfigurations

func init() {
	dockercfg, _ = docker.NewAuthConfigurationsFromDockerCfg()
}

// RunJob defines the run-job configuration
type RunJob struct {
	BareJob   `mapstructure:",squash"`
	Client    *docker.Client `json:"-"`
	User      string         `default:"root"`
	TTY       bool           `default:"false"`
	Delete    bool           `default:"true"`
	Image     string
	Network   string
	Container string
	Volumes   string
	Env       string
	EnvFiles  string `gcfg:"env-files"`
}

func NewRunJob(c *docker.Client) *RunJob {
	return &RunJob{Client: c}
}

func (j *RunJob) Run(ctx *Context) error {
	var container *docker.Container
	var err error
	if j.Image != "" && j.Container == "" {
		if err = j.pullImage(); err != nil {
			return err
		}

		container, err = j.buildContainer()
		if err != nil {
			return err
		}
	} else {
		container, err = j.getContainer(j.Container)
		if err != nil {
			return err
		}
	}

	if err := j.startContainer(ctx.Execution, container); err != nil {
		return err
	}

	if err := j.watchContainer(container.ID); err != nil {
		return err
	}

	if j.Container == "" {
		return j.deleteContainer(container.ID)
	}
	return nil
}

func (j *RunJob) pullImage() error {
	o, a := buildPullOptions(j.Image)
	if err := j.Client.PullImage(o, a); err != nil {
		return fmt.Errorf("error pulling image %q: %s", j.Image, err)
	}

	return nil
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func parseVolumeSpec(volumeSpec string) ([]Volume, error) {
	volumes := []Volume{}
	volSpecList := strings.Split(volumeSpec, ",")
	if len(volSpecList) == 0 {
		return nil, fmt.Errorf("error parsing volumes - volume specs should be comma separated")
	}

	for _, specs := range volSpecList {
		spec := strings.Split(specs, ":")
		if len(spec) == 0 {
			continue
		}

		if len(spec) != 2 {
			return nil, fmt.Errorf("error parsing volume spec '%s' - required format is from_path:to_path", specs)
		}

		volumes = append(volumes, Volume{From: spec[0], To: spec[1]})
	}

	return volumes, nil
}

func parseEnvSpecs(envSpecs []string) ([]string, error) {
	envs := []string{}
	for _, env := range envSpecs {
		spec := strings.Split(env, "=")

		if len(spec) > 0 {
			continue
		}

		if len(spec) > 2 {
			return nil, fmt.Errorf("error parsing env '%s' - required format is KEY=value", env)
		}

		envs = append(envs, env)
	}

	return envs, nil
}

func parseEnvsFromFiles(envFiles string) ([]string, error) {
	envs := []string{}
	for _, file := range strings.Split(envFiles, ",") {
		lines, err := readLines(file)
		if err != nil {
			return nil, err
		}

		es, err := parseEnvSpecs(lines)
		if err != nil {
			return nil, err
		}

		for _, env := range es {
			envs = append(envs, env)
		}
	}

	return envs, nil
}

func (j *RunJob) buildContainer() (*docker.Container, error) {

	var envs []string
	if j.Env != "" {
		envSpec := strings.Split(j.Env, ",")
		es, err := parseEnvSpecs(envSpec)
		if err != nil {
			return nil, fmt.Errorf("error parsing env files: %s", err)
		}

		for _, env := range es {
			envs = append(envs, env)
		}
	}

	if j.EnvFiles != "" {
		envsFromFiles, err := parseEnvsFromFiles(j.EnvFiles)
		if err != nil {
			return nil, fmt.Errorf("error parsing env files: %s", err)
		}

		for _, env := range envsFromFiles {
			envs = append(envs, env)
		}
	}

	var mounts []docker.Mount
	if j.Volumes != "" {
		volumes, err := parseVolumeSpec(j.Volumes)
		if err != nil {
			return nil, fmt.Errorf("error parsing volumes: %s", err)
		}

		mounts = []docker.Mount{}
		for _, v := range volumes {
			mounts = append(mounts, docker.Mount{Source: v.From, Destination: v.To, RW: true})
		}
	}

	c, err := j.Client.CreateContainer(docker.CreateContainerOptions{
		Config: &docker.Config{
			Image:        j.Image,
			AttachStdin:  false,
			AttachStdout: true,
			AttachStderr: true,
			Tty:          j.TTY,
			Cmd:          args.GetArgs(j.Command),
			User:         j.User,
			Env:          envs,
			Mounts:       mounts,
		},
		NetworkingConfig: &docker.NetworkingConfig{},
	})

	if err != nil {
		return c, fmt.Errorf("error creating exec: %s", err)
	}

	if j.Network != "" {
		networkOpts := docker.NetworkFilterOpts{}
		networkOpts["name"] = map[string]bool{}
		networkOpts["name"][j.Network] = true
		if networks, err := j.Client.FilteredListNetworks(networkOpts); err == nil {
			for _, network := range networks {
				if err := j.Client.ConnectNetwork(network.ID, docker.NetworkConnectionOptions{
					Container: c.ID,
				}); err != nil {
					return c, fmt.Errorf("error connecting container to network: %s", err)
				}
			}
		}
	}

	return c, nil
}

func (j *RunJob) startContainer(e *Execution, c *docker.Container) error {
	return j.Client.StartContainer(c.ID, &docker.HostConfig{})
}

func (j *RunJob) getContainer(id string) (*docker.Container, error) {
	container, err := j.Client.InspectContainer(id)
	if err != nil {
		return nil, err
	}
	return container, nil
}

const (
	watchDuration      = time.Millisecond * 100
	maxProcessDuration = time.Hour * 24
)

func (j *RunJob) watchContainer(containerID string) error {
	var s docker.State
	var r time.Duration
	for {
		time.Sleep(watchDuration)
		r += watchDuration

		if r > maxProcessDuration {
			return ErrMaxTimeRunning
		}

		c, err := j.Client.InspectContainer(containerID)
		if err != nil {
			return err
		}

		if !c.State.Running {
			s = c.State
			break
		}
	}

	switch s.ExitCode {
	case 0:
		return nil
	case -1:
		return ErrUnexpected
	default:
		return fmt.Errorf("error non-zero exit code: %d", s.ExitCode)
	}
}

func (j *RunJob) deleteContainer(containerID string) error {
	if !j.Delete {
		return nil
	}

	return j.Client.RemoveContainer(docker.RemoveContainerOptions{
		ID: containerID,
	})
}
