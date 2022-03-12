export GITHUB_TOKEN=""
gpg --list-keys
export GPG_FINGERPRINT=""
export GPG_TTY=$(tty)
goreleaser release --rm-dist
