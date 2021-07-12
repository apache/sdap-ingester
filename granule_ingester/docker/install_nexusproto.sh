set -e

#APACHE_NEXUSPROTO="https://github.com/apache/incubator-sdap-nexusproto.git"
APACHE_NEXUSPROTO="https://github.com/wphyojpl/incubator-sdap-nexusproto.git"
MASTER="master"

GIT_REPO=${1:-$APACHE_NEXUSPROTO}
GIT_BRANCH=${2:-$MASTER}

mkdir nexusproto
cd nexusproto
git init
git pull ${GIT_REPO} ${GIT_BRANCH}

./gradlew pythonInstall --info

./gradlew install --info

rm -rf /root/.gradle
cd ..
rm -rf nexusproto
