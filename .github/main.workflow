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
  uses = "ghetzel/go-release.action@d2a93b3e6c7c606f23c1d6dc07797b357e455fa5"
  env = {
    GOOS = "darwin"
    GOARCH = "amd64"
  }
  secrets = ["GITHUB_TOKEN"]
}

action "release freebsd/amd64" {
  uses = "ghetzel/go-release.action@d2a93b3e6c7c606f23c1d6dc07797b357e455fa5"
  env = {
    GOOS = "freebsd"
    GOARCH = "amd64"
  }
  secrets = ["GITHUB_TOKEN"]
}

action "release linux/amd64" {
  uses = "ghetzel/go-release.action@d2a93b3e6c7c606f23c1d6dc07797b357e455fa5"
  env = {
    GOOS = "linux"
    GOARCH = "amd64"
  }
  secrets = ["GITHUB_TOKEN"]
}