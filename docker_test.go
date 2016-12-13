package goworker

import (
	"testing"
	"os/exec"
	"strings"
	"time"
	"fmt"
)

type container struct {
	container string
	address   string
	image     string
}

type containerSetup map[string][]container

func dockerComposeUp(t *testing.T) {
	t.Log("Starting containers")
	err := exec.Command("docker-compose", "up", "-d").Run()
	if err != nil {
		t.Fatal(err)
	}
}

func dockerComposeScaleSentinels(t *testing.T) {
	t.Log("Scaling sentinels to 3")
	err := exec.Command("docker-compose", "scale", "sentinel=3").Run()
	if err != nil {
		t.Fatal(err)
	}
}

func scanSetup(t *testing.T) containerSetup {
	t.Log("Mapping containers")
	out, err := exec.Command(
		"docker",
		"ps",
		"--format",
		"{{.Image}} {{.Names}}",
	).Output()
	if err != nil {
		t.Fatal(err)
	}

	schema := containerSetup{}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, " ")

		out, err := exec.Command(
			"docker",
			"inspect",
			"--format",
			"{{.NetworkSettings.IPAddress}}",
			parts[1],
		).Output()
		if err != nil {
			t.Fatal(err)
		}

		image := parts[0]
		imageParts := strings.Split(image, "_")
		image = imageParts[len(imageParts)-1]
		if _, ok := schema[image]; !ok {
			schema[image] = []container{}
		}
		schema[image] = append(
			schema[image],
			container{
				container: parts[1],
				address:   strings.TrimSpace(string(out)),
				image:     image,
			},
		)
	}

	return schema
}

func verifySetupSize(setup containerSetup, t *testing.T) {
	t.Log("Verifying container setup")
	for image, count := range map[string]int{
		"master":   1,
		"slave":    1,
		"sentinel": 3,
	} {
		containers, ok := setup[image]
		if !ok {
			t.Fatalf("Missing container for image %v", image)
		}
		l := len(containers)
		if l != count {
			t.Fatalf("Expected %v containers for image %v, actual %v", count, image, l)
		}
	}
}

func ensureMaster(setup containerSetup, t *testing.T) {
	t.Log("Verifying that master container serves as Redis master")
	image, _ := dockerGetMaster(setup, t)
	if image != "master" {
		t.Log("Slave container serves as Redis master, forcing failover by pausing slaves")
		for _, c := range setup["slave"] {
			pauseContainer(c.container, t)
		}
		time.Sleep(3 * FailoverTimeout)
		image, _ := dockerGetMaster(setup, t)
		if image != "master" {
			t.Fatal("Master image still not redis master, deal with it yourself")
		}
	}
	dockerComposeUnpause(setup, t)
}

func dockerGetMaster(setup containerSetup, t *testing.T) (string, string) {
	master := ""
	for _, sentinel := range setup["sentinel"] {
		out, err := exec.Command(
			"docker",
			"exec",
			sentinel.container,
			"redis-cli",
			"-p",
			fmt.Sprint(SentinelPort),
			"sentinel",
			"get-master-addr-by-name",
			MasterSetName,
		).Output()
		s := string(out)
		if s != "" {
			master = s
			break
		}
		if err != nil && err.Error() != "exit status 1" {
			t.Fatal(err)
		}
	}
	address := strings.Split(strings.TrimSpace(master), "\n")[0]

	for _, c := range append(setup["master"], setup["slave"]...) {
		if c.address == address {
			return c.image, c.container
		}
	}
	return "", ""
}

func pauseContainer(container string, t *testing.T) {
	err := exec.Command("docker", "pause", container).Run()
	if err != nil {
		t.Fatal(err)
	}
}

func dockerComposeUnpause(setup containerSetup, t *testing.T) {
	t.Log("Unpausing any paused containers")
	args := []string{"unpause"}
	for _, containers := range setup {
		for _, container := range containers {
			args = append(args, container.container)
		}
	}
	if err := exec.Command("docker", args...).Run(); err != nil && err.Error() != "exit status 1" {
		t.Fatal(err)
	}
}

func dockerClearDb(setup containerSetup, t *testing.T) {
	t.Log("Clearing Redis")
	container := setup["master"][0].container
	if err := exec.Command(
		"docker",
		"exec",
		container,
		"redis-cli",
		"FLUSHALL",
	).Run(); err != nil {
		t.Fatal(err)
	}
}

func dockerComposeStop(t *testing.T) {
	func() {
		t.Log("Stopping containers")
		exec.Command("docker-compose", "stop").Run()
	}()
}

