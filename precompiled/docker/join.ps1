# Re-assemble the image tarball from its parts (GitHub file-size limit) and verify it (Windows PowerShell 5+ / 7).
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$f = 'ha-apilog-elt-1.0.0-linux-amd64.tar.gz'
$out = [System.IO.File]::Create((Join-Path $PSScriptRoot $f))
foreach ($p in (Get-ChildItem "$f.part-*" | Sort-Object Name)) { $b = [System.IO.File]::ReadAllBytes($p.FullName); $out.Write($b, 0, $b.Length) }
$out.Close()
$want = ((Get-Content ..\SHA256SUMS) -match " docker/$([regex]::Escape($f))$" -split ' ')[0]
$have = (Get-FileHash -Algorithm SHA256 $f).Hash.ToLower()
if ($want -ne $have) { throw "checksum mismatch: $have" } else { "OK $f" }
"load with: docker load -i $f"
