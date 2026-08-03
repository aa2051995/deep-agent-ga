// Deep Agent GA — CI/CD pipeline (Jenkins, in-cluster on EKS)
// =============================================================================
// Model (see deploy/cicd/README.md for the full setup):
//   * Jenkins agents run as PODS on the EKS `deep-agent-ga` cluster (Kubernetes plugin).
//   * The agent pod's ServiceAccount `jenkins-agent` is IRSA-annotated with an
//     IAM role that can PUSH to ECR (kaniko builds+pushes, no docker daemon).
//   * The SAME ServiceAccount is granted in-cluster RBAC in the `deep-agent-ga`
//     namespace, so `helm`/`kubectl` deploy using the pod's in-cluster config —
//     no `aws eks update-kubeconfig`, no static kubeconfig.
//
// Flow:
//   * Pull request  -> Validate (helm lint/render + unit tests) + Build (--no-push).
//   * Merge to main -> Validate + Build & PUSH (<git-sha> and :latest) + Deploy
//                      (helm upgrade --install, image.tag=<git-sha>) + Verify rollout.
//
// Multibranch job config: set "Script Path" = examples/deep_research/Jenkinsfile.
// =============================================================================

def podYaml = '''
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: deep-agent-ga-ci
spec:
  serviceAccountName: jenkins-agent   # IRSA (ECR push) + in-cluster RBAC (helm deploy)
  containers:
    - name: kaniko
      image: gcr.io/kaniko-project/executor:v1.23.2-debug
      command: ["/busybox/cat"]
      tty: true
      resources:
        requests: { cpu: "500m", memory: 1Gi }
        limits:   { cpu: "2",    memory: 4Gi }
    - name: tools           # helm + kubectl (+ python for chart render tests)
      # Pin to an available helm-kubectl release; bump if the tag 404s on pull.
      image: dtzar/helm-kubectl:3.15.4
      command: ["cat"]
      tty: true
      resources:
        requests: { cpu: "200m", memory: 256Mi }
        limits:   { cpu: "1",    memory: 1Gi }
    - name: python          # pipeline + chart unit tests
      image: python:3.11-slim
      command: ["cat"]
      tty: true
      resources:
        requests: { cpu: "200m", memory: 256Mi }
        limits:   { cpu: "1",    memory: 1Gi }
'''

pipeline {
  agent {
    kubernetes {
      defaultContainer 'tools'
      yaml podYaml
    }
  }

  options {
    timestamps()
    disableConcurrentBuilds()
    timeout(time: 30, unit: 'MINUTES')
    buildDiscarder(logRotator(numToKeepStr: '30'))
  }

  environment {
    APP_DIR      = 'examples/deep_research'
    REGISTRY     = '553138586148.dkr.ecr.us-east-1.amazonaws.com'
    REGION       = 'us-east-1'
    CLUSTER      = 'deep-agent-ga'
    NAMESPACE    = 'deep-agent-ga'
    RELEASE      = 'deep-agent-ga'
    BACKEND_REPO = 'deep-agent-ga-backend'   // apiserver + worker share this image
    UI_REPO      = 'deep-agent-ga-ui'
    CHART        = 'examples/deep_research/deploy/helm/deep-agent-ga'
    VALUES       = 'examples/deep_research/deploy/helm/deep-agent-ga/values-aws.yaml'
  }

  stages {
    stage('Setup') {
      steps {
        script {
          // Immutable, traceable tag = short git SHA. `latest` is also pushed on main.
          env.IMAGE_TAG = sh(returnStdout: true, script: 'git rev-parse --short=12 HEAD').trim()
          env.IS_MAIN   = (env.BRANCH_NAME == 'main').toString()
          echo "branch=${env.BRANCH_NAME}  tag=${env.IMAGE_TAG}  deploy=${env.IS_MAIN}"
        }
      }
    }

    stage('Validate') {
      parallel {
        stage('Helm lint & render') {
          steps {
            container('tools') {
              sh '''
                set -eu
                helm lint "${CHART}" -f "${VALUES}"
                # Render with the exact tags the deploy would use — proves the
                # chart templates cleanly before we build anything.
                helm template "${RELEASE}" "${CHART}" -f "${VALUES}" \
                  --set global.imageRegistry="${REGISTRY}" \
                  --set apiserver.image.tag="${IMAGE_TAG}" \
                  --set worker.image.tag="${IMAGE_TAG}" \
                  --set ui.image.tag="${IMAGE_TAG}" > /dev/null
                echo "helm lint + template OK"
              '''
            }
          }
        }
        stage('Pipeline unit tests') {
          steps {
            container('python') {
              sh '''
                set -eu
                pip install --quiet --no-cache-dir pyyaml pytest
                cd "${APP_DIR}"
                python -m pytest deploy/cicd/tests -q
              '''
            }
          }
        }
      }
    }

    stage('Chart render tests') {
      steps {
        container('tools') {
          // helm-kubectl is Alpine; add python so the existing chart render
          // tests (which shell out to `helm template`) can run in-container.
          sh '''
            set -eu
            apk add --no-cache python3 py3-pip py3-yaml >/dev/null
            pip install --quiet --no-cache-dir --break-system-packages pytest >/dev/null 2>&1 \
              || pip install --quiet --no-cache-dir pytest
            cd "${APP_DIR}"
            python3 -m pytest deploy/helm/deep-agent-ga/tests -q
          '''
        }
      }
    }

    stage('Build images') {
      steps {
        container('kaniko') {
          script {
            buildImage(env.BACKEND_REPO, "${env.APP_DIR}/stream-backend")
            buildImage(env.UI_REPO,      "${env.APP_DIR}/ui")
          }
        }
      }
    }

    stage('Deploy') {
      when { expression { env.IS_MAIN == 'true' } }
      steps {
        container('tools') {
          // Model/tool API keys come from Jenkins credentials, not the repo.
          // (Alternative: pre-create a Secret and set secrets.existingSecret —
          //  see deploy/cicd/README.md.)
          withCredentials([
            string(credentialsId: 'deep-agent-ga-tavily-api-key',    variable: 'TAVILY_API_KEY'),
            string(credentialsId: 'deep-agent-ga-anthropic-api-key',  variable: 'ANTHROPIC_API_KEY')
          ]) {
            sh '''
              set -eu
              helm upgrade --install "${RELEASE}" "${CHART}" \
                -n "${NAMESPACE}" \
                -f "${VALUES}" \
                --set global.imageRegistry="${REGISTRY}" \
                --set apiserver.image.tag="${IMAGE_TAG}" \
                --set worker.image.tag="${IMAGE_TAG}" \
                --set ui.image.tag="${IMAGE_TAG}" \
                --set-string secrets.tavilyApiKey="${TAVILY_API_KEY}" \
                --set-string secrets.anthropicApiKey="${ANTHROPIC_API_KEY}" \
                --wait --timeout 10m
            '''
          }
        }
      }
    }

    stage('Verify rollout') {
      when { expression { env.IS_MAIN == 'true' } }
      steps {
        container('tools') {
          sh '''
            set -eu
            for d in apiserver worker ui; do
              kubectl rollout status "deploy/${RELEASE}-${d}" -n "${NAMESPACE}" --timeout=5m
            done
            kubectl get pods,ingress -n "${NAMESPACE}"
          '''
        }
      }
    }
  }

  post {
    success {
      echo "OK  branch=${env.BRANCH_NAME}  tag=${env.IMAGE_TAG}  deployed=${env.IS_MAIN}"
    }
    failure {
      echo "FAILED  branch=${env.BRANCH_NAME}  build=${env.BUILD_URL}"
    }
  }
}

// Build (and, on main only, push) one image with kaniko. ECR auth is picked up
// automatically from the pod's IRSA credentials — no explicit registry login,
// no docker daemon.
def buildImage(String repo, String context) {
  def dest    = "${env.REGISTRY}/${repo}"
  def ws       = env.WORKSPACE
  def pushArgs = (env.IS_MAIN == 'true')
      ? "--destination ${dest}:${env.IMAGE_TAG} --destination ${dest}:latest"
      : "--no-push --destination ${dest}:${env.IMAGE_TAG}"
  sh """
    set -eu
    /kaniko/executor \
      --context dir://${ws}/${context} \
      --dockerfile ${ws}/${context}/Dockerfile \
      ${pushArgs} \
      --cache=true \
      --cache-ttl=168h \
      --snapshot-mode=redo \
      --use-new-run
  """
}
