param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$csc = 'C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe'
$runtimeRoot = 'C:\Program Files\dotnet\shared\Microsoft.NETCore.App'

if (-not (Test-Path -LiteralPath $csc)) {
    throw "C# compiler not found: $csc"
}

$runtime = Get-ChildItem -LiteralPath $runtimeRoot -Directory |
    Where-Object { $_.Name -like '6.*' } |
    Sort-Object { [version]$_.Name } -Descending |
    Select-Object -First 1
if ($null -eq $runtime) {
    throw "Microsoft.NETCore.App 6.x runtime not found: $runtimeRoot"
}

$assemblies = @(
    'System.Private.CoreLib.dll',
    'System.Runtime.dll',
    'System.Console.dll',
    'System.IO.FileSystem.dll',
    'System.IO.Compression.dll',
    'System.Reflection.dll',
    'System.Reflection.DispatchProxy.dll',
    'System.Collections.dll',
    'System.Collections.NonGeneric.dll',
    'System.Memory.dll',
    'System.Threading.Tasks.dll',
    'System.Text.Encoding.Extensions.dll',
    'System.Text.Json.dll'
)
$references = foreach ($assembly in $assemblies) {
    $path = Join-Path $runtime.FullName $assembly
    if (-not (Test-Path -LiteralPath $path)) {
        throw "Runtime reference not found: $path"
    }
    "/reference:`"$path`""
}

& $csc /nologo /noconfig /nostdlib+ /target:exe /optimize+ `
    /out:"$root\YuanhangBridge.dll" $references "$root\YuanhangBridge.cs"
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Output "Built $root\YuanhangBridge.dll"
