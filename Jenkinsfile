pipeline {
  agent any

  environment {
    GIT_NAME = "eea.odpckan"
    SONARQUBE_TAGS = "www.eea.europa.eu"
  }

  stages {

    stage('Cosmetics') {
      steps {
        parallel(

          "PEP8": {
            node(label: 'docker') {
              script {
                catchError(buildResult: 'SUCCESS', stageResult: 'UNSTABLE') {
                  sh '''docker run -i --rm --name="$BUILD_TAG-pep8" -e GIT_SRC="https://github.com/eea/$GIT_NAME.git" -e GIT_NAME="$GIT_NAME" -e GIT_BRANCH="$BRANCH_NAME" -e GIT_CHANGE_ID="$CHANGE_ID" eeacms/pep8'''
                }
              }
            }
          },

          "PyLint": {
            node(label: 'docker') {
              script {
                catchError(buildResult: 'SUCCESS', stageResult: 'UNSTABLE') {
                  sh '''docker run -i --rm --name="$BUILD_TAG-pylint" -e GIT_SRC="https://github.com/eea/$GIT_NAME.git" -e GIT_NAME="$GIT_NAME" -e GIT_BRANCH="$BRANCH_NAME" -e GIT_CHANGE_ID="$CHANGE_ID" eeacms/pylint'''
                }
              }
            }
          }

        )
      }
    }

    stage('Code') {
      steps {
        parallel(

          "PyFlakes": {
            node(label: 'docker') {
              sh '''docker run -i --rm --name="$BUILD_TAG-pyflakes" -e GIT_SRC="https://github.com/eea/$GIT_NAME.git" -e GIT_NAME="$GIT_NAME" -e GIT_BRANCH="$BRANCH_NAME" -e GIT_CHANGE_ID="$CHANGE_ID" eeacms/pyflakes'''
            }
          }

        )
      }
    }

    stage('Build & Test') {
      steps {
        node(label: 'clair') {
          script {
            try {
              checkout scm
              sh "docker build -t ${BUILD_TAG} ."
              sh '''docker run --rm --volume app:/app -e SERVICES_SDS=http://example.com -e CKAN_ADDRESS=http://example.com -i ${BUILD_TAG} sh -c "cd /app; pip install -r requirements-dev.txt; exec pytest -vv --cov-report=xml --junitxml=xunit-report.xml"'''
            } finally {
              sh "docker rmi ${BUILD_TAG}"
            }
          }
        }
      }
    }

    stage('Report to SonarQube') {
      when {
        allOf {
          environment name: 'CHANGE_ID', value: ''
        }
      }
      steps {
        node(label: 'swarm') {
          script{
            checkout scm
            dir("xunit-reports") {
              unstash "xunit-reports"
            }
            def scannerHome = tool 'SonarQubeScanner';
            def nodeJS = tool 'NodeJS11';
            withSonarQubeEnv('Sonarqube') {
                sh "export PATH=$PATH:${scannerHome}/bin:${nodeJS}/bin; sonar-scanner -Dsonar.python.xunit.skipDetails=true -Dsonar.python.xunit.reportPath=./app/xunit-report.xml -Dsonar.python.coverage.reportPath=./app/coverage.xml -Dsonar.sources=./app -Dsonar.projectKey=$GIT_NAME-$BRANCH_NAME -Dsonar.projectVersion=$BRANCH_NAME-$BUILD_NUMBER"
                sh '''try=2; while [ \$try -gt 0 ]; do curl -s -XPOST -u "${SONAR_AUTH_TOKEN}:" "${SONAR_HOST_URL}api/project_tags/set?project=${GIT_NAME}-${BRANCH_NAME}&tags=${SONARQUBE_TAGS},${BRANCH_NAME}" > set_tags_result; if [ \$(grep -ic error set_tags_result ) -eq 0 ]; then try=0; else cat set_tags_result; echo "... Will retry"; sleep 60; try=\$(( \$try - 1 )); fi; done'''
            }
          }
        }
      }
    }
  }

  post {
    changed {
      script {
        def url = "${env.BUILD_URL}/display/redirect"
        def status = currentBuild.currentResult
        def subject = "${status}: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'"
        def summary = "${subject} (${url})"
        def details = """<h1>${env.JOB_NAME} - Build #${env.BUILD_NUMBER} - ${status}</h1>
                         <p>Check console output at <a href="${url}">${env.JOB_BASE_NAME} - #${env.BUILD_NUMBER}</a></p>
                      """

        def color = '#FFFF00'
        if (status == 'SUCCESS') {
          color = '#00FF00'
        } else if (status == 'FAILURE') {
          color = '#FF0000'
        }

        emailext (subject: '$DEFAULT_SUBJECT', to: '$DEFAULT_RECIPIENTS', body: details)
      }
    }
  }
}
