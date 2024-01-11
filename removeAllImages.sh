docker image ls -q -f "reference=sg_intel_vendas*" | xargs -I {} docker image rm -f {}

