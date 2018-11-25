build:
	go build

cross-compile:
	gox -os="linux" -arch="386 amd64 arm arm64" -output="dist/{{.Dir}}_{{.OS}}_{{.Arch}}/{{.Dir}}"
	gox -os="darwin" -arch="386 amd64" -output="dist/{{.Dir}}_{{.OS}}_{{.Arch}}/{{.Dir}}"
	gox -os="windows" -arch="386 amd64" -output="dist/{{.Dir}}_{{.OS}}_{{.Arch}}/{{.Dir}}"

package:
	goxc

release: clean cross-compile package
	ghr -draft -token ${GITHUB_TOKEN} ${VERSION} dist/snapshot/
	@echo "released as draft"

clean:
	rm -f spanner-cli
	rm -rf dist/
	go clean -testcache

run:
	./spanner-cli -p ${PROJECT} -i ${INSTANCE} -d ${DATABASE}

test:
	@SPANNER_CLI_INTEGRATION_TEST_PROJECT_ID=${PROJECT} SPANNER_CLI_INTEGRATION_TEST_INSTANCE_ID=${INSTANCE} SPANNER_CLI_INTEGRATION_TEST_DATABASE_ID=${DATABASE} SPANNER_CLI_INTEGRATION_TEST_CREDENTIAL=${CREDENTIAL} GOOGLE_APPLICATION_CREDENTIALS=/dev/null go test -v ./...
