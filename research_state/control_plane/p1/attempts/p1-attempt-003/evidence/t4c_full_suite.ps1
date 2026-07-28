$ErrorActionPreference = "Stop"

$env:AG2_OPENAI_API_KEY = "T4C_TEST_ONLY_NON_SECRET"
$env:AG2_OPENAI_BASE_URL = "https://example.invalid/v1"
$env:AG2_OPENAI_MODEL = "t4c-test-gpt-model"

$env:AG2_DEEPSEEK2_API_KEY = "T4C_TEST_ONLY_NON_SECRET"

$env:AG2_Kimi_API_KEY = "T4C_TEST_ONLY_NON_SECRET"
$env:AG2_Kimi_BASE_URL = "https://example.invalid/v1"
$env:AG2_Kimi_MODEL = "t4c-test-kimi-model"

$env:AG2_ZHIPU_API_KEY = "T4C_TEST_ONLY_NON_SECRET"
$env:AG2_ZHIPU_BASE_URL = "https://example.invalid/v1"
$env:AG2_ZHIPU_MODEL = "t4c-test-glm-model"

& "C:/Users/Administrator/AppData/Local/Programs/Python/Python313/python.exe" `
    -B -m unittest discover -s tests -q
exit $LASTEXITCODE
