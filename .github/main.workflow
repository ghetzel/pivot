# .github/main.workflow

workflow "Build" {
  on = "release"
  resolves = [
    "release darwin/amd64",
    "release freebsd/amd64",
    "release linux/amd64",
  ]
}

action "release darwin/amd64" {
  uses = "ghetzel/go-release.action@v1.12"
  env = {
    GOOS = "darwin"
    GOARCH = "amd64"
  }
  secrets = ["GITHUB_TOKEN"]
}

action "release freebsd/amd64" {
  uses = "ghetzel/go-release.action@v1.12"
  env = {
    GOOS = "freebsd"
    GOARCH = "amd64"
  }
  secrets = ["GITHUB_TOKEN"]
}

action "release linux/amd64" {
  uses = "ghetzel/go-release.action@v1.12"
  env = {
    GOOS = "linux"
    GOARCH = "amd64"
  }
  secrets = ["GITHUB_TOKEN"]
}
