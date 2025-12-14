# Checks if an env var exists and is not just whitespace
def validate-env [name: string] {
    let raw_value = ($env | get -o $name)
    # print $"Variable '($name)' value: ($raw_value)"
    if ($raw_value == null) {
        return false
    }

    not ($raw_value | str trim | is-empty)
}

def check-env-value [name: string] {
  if not (validate-env $name) {
    print $"Variable '($name)' not found!"
    exit 1
  }
  return ($env | get -o $name)
}

def namespace-exists [name: string] {
  let ns: string = do { kubectl get --ignore-not-found=true ns $name};
  if not ("Active" in $ns) {
    return false  
  }
  true 
}

let namespace: string = "dagster"
let user: string = check-env-value ("DAGSTER_POSTGRESQL_USER")
let password: string = check-env-value ("DAGSTER_POSTGRESQL_PASSWORD")

if not (namespace-exists $namespace) {
  print $"Namespace '($namespace)' do not exist! Creating namespace."
  kubectl create namespace $namespace
}

if not ("sealed-secrets" | path exists) {
  mkdir sealed-secrets
}

print "Creating sealed secret for Dagster user..."
(
  kubectl create secret generic postgres-secrets
  --from-literal=$"postgresql-username=($user)"
  --from-literal=$"postgresql-password=($password)"
  -n $namespace
  --dry-run=client -o yaml |
  kubeseal -o yaml |
  save -f manifests/sealed-secrets/dagster-secrets-sealed.yaml
)

kubectl apply -f manifests/sealed-secrets/dagster-secrets-sealed.yaml
