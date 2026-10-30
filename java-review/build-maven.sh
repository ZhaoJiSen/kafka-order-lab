#!/usr/bin/env bash
set -euo pipefail
mkdir -p /root/.m2
cat > /root/.m2/settings.xml <<'EOF'
<settings><mirrors><mirror><id>aliyun-public</id><mirrorOf>central</mirrorOf><url>https://maven.aliyun.com/repository/public</url></mirror></mirrors></settings>
EOF
mvn -U -B -q -Dhttps.protocols=TLSv1.2 -Dmaven.wagon.http.retryHandler.count=3 "$@"
