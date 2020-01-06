package core

import (
	"archive/tar"
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/fsouza/go-dockerclient/testing"
	logging "github.com/op/go-logging"
	. "gopkg.in/check.v1"
)

const (
	ImageFixture = "test-image"
	VolumeSpec   = "/data:/data,/another/path:/config"
	EnvVars      = "KEY1=val1,KEY2=val2"
	EnvSpecFiles = "../common.env,../another.env"
)

type SuiteRunJob struct {
	server *testing.DockerServer
	client *docker.Client
}

var _ = Suite(&SuiteRunJob{})

func (s *SuiteRunJob) SetUpTest(c *C) {
	var err error
	s.server, err = testing.NewServer("127.0.0.1:0", nil, nil)
	c.Assert(err, IsNil)

	s.client, err = docker.NewClient(s.server.URL())
	c.Assert(err, IsNil)

	volumes := []string{}
	for _, vs := range strings.Split(VolumeSpec, ",") {
		spec := strings.Split(vs, ":")
		volumes = append(volumes, spec[0])
	}

	s.buildImage(c)
	s.createNetwork(c)
	s.createVolumes(c, volumes)
	err = s.createEnvFiles()
	c.Assert(err, IsNil)
}

func (s *SuiteRunJob) TestRun(c *C) {
	job := &RunJob{Client: s.client}
	job.Image = ImageFixture
	job.Command = `echo -a "foo bar"`
	job.User = "foo"
	job.TTY = true
	job.Delete = true
	job.Network = "foo"
	job.Volumes = VolumeSpec
	job.Env = EnvVars
	job.EnvFiles = EnvSpecFiles

	e := NewExecution()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		time.Sleep(time.Millisecond * 200)

		containers, err := s.client.ListContainers(docker.ListContainersOptions{})
		c.Assert(err, IsNil)
		c.Assert(containers[0].Command, Equals, "echo -a foo bar")
		c.Assert(containers[0].Status[:2], Equals, "Up")

		err = s.client.StopContainer(containers[0].ID, 0)
		c.Assert(err, IsNil)
		wg.Done()
	}()

	var logger Logger
	logging.SetFormatter(logging.MustStringFormatter(logFormat))
	logger = logging.MustGetLogger("ofelia")

	err := job.Run(&Context{Execution: e, Logger: logger})
	c.Assert(err, IsNil)
	wg.Wait()

	containers, err := s.client.ListContainers(docker.ListContainersOptions{
		All: true,
	})
	c.Assert(err, IsNil)
	c.Assert(containers, HasLen, 0)
}

func (s *SuiteRunJob) TestBuildPullImageOptionsBareImage(c *C) {
	o, _ := buildPullOptions("foo")
	c.Assert(o.Repository, Equals, "foo")
	c.Assert(o.Tag, Equals, "latest")
	c.Assert(o.Registry, Equals, "")
}

func (s *SuiteRunJob) TestBuildPullImageOptionsVersion(c *C) {
	o, _ := buildPullOptions("foo:qux")
	c.Assert(o.Repository, Equals, "foo")
	c.Assert(o.Tag, Equals, "qux")
	c.Assert(o.Registry, Equals, "")
}

func (s *SuiteRunJob) TestBuildPullImageOptionsRegistry(c *C) {
	o, _ := buildPullOptions("quay.io/srcd/rest:qux")
	c.Assert(o.Repository, Equals, "quay.io/srcd/rest")
	c.Assert(o.Tag, Equals, "qux")
	c.Assert(o.Registry, Equals, "quay.io")
}

func (s *SuiteRunJob) TestBuildPullImageOptionsRegistryWithPort(c *C) {
	o, _ := buildPullOptions("quay.io:5000/srcd/rest:qux")
	c.Assert(o.Repository, Equals, "quay.io:5000/srcd/rest")
	c.Assert(o.Tag, Equals, "qux")
	c.Assert(o.Registry, Equals, "quay.io:5000")
}

func (s *SuiteRunJob) TestBuildPullImageOptionsRegistryWithPortNoTag(c *C) {
	o, _ := buildPullOptions("quay.io:5000/srcd/rest")
	c.Assert(o.Repository, Equals, "quay.io:5000/srcd/rest")
	c.Assert(o.Tag, Equals, "latest")
	c.Assert(o.Registry, Equals, "quay.io:5000")
}

func (s *SuiteRunJob) TestBuildPullImageOptionsRegistryWithPortSimpleRepository(c *C) {
	o, _ := buildPullOptions("quay.io:5000/srcd:qux")
	c.Assert(o.Repository, Equals, "quay.io:5000/srcd")
	c.Assert(o.Tag, Equals, "qux")
	c.Assert(o.Registry, Equals, "quay.io:5000")
}

func (s *SuiteRunJob) TestBuildPullImageOptionsRegistryWithPortSimpleRepositoryNoTag(c *C) {
	o, _ := buildPullOptions("quay.io:5000/srcd")
	c.Assert(o.Repository, Equals, "quay.io:5000/srcd")
	c.Assert(o.Tag, Equals, "latest")
	c.Assert(o.Registry, Equals, "quay.io:5000")
}

func (s *SuiteRunJob) buildImage(c *C) {
	inputbuf := bytes.NewBuffer(nil)
	tr := tar.NewWriter(inputbuf)
	tr.WriteHeader(&tar.Header{Name: "Dockerfile"})
	tr.Write([]byte("FROM base\n"))
	tr.Close()

	err := s.client.BuildImage(docker.BuildImageOptions{
		Name:         ImageFixture,
		InputStream:  inputbuf,
		OutputStream: bytes.NewBuffer(nil),
	})
	c.Assert(err, IsNil)
}

func (s *SuiteRunJob) createNetwork(c *C) {
	_, err := s.client.CreateNetwork(docker.CreateNetworkOptions{
		Name:   "foo",
		Driver: "bridge",
	})
	c.Assert(err, IsNil)
}

func (s *SuiteRunJob) createVolumes(c *C, volumes []string) {
	for _, volume := range volumes {
		_, err := s.client.CreateVolume(docker.CreateVolumeOptions{
			Name: volume,
		})
		c.Assert(err, IsNil)
	}
}

func (s *SuiteRunJob) createEnvFiles() error {
	for i, path := range strings.Split(EnvSpecFiles, ",") {
		file, err := os.Create(path)
		if err != nil {
			return err
		}

		w := bufio.NewWriter(file)

		for j := 1; j <= 4; j++ {
			fmt.Fprintf(w, "FILE%dKEY%d=VALUE\n", i, j)
		}

		err = w.Flush()
		file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
