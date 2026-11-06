# deploy/agent — packaging and scripts for the ALELT Agent

| Path | Purpose |
|---|---|
| `wix/Package.wxs` | WiX v4 MSI: installs `aleltagent.exe` + `aleltsrc_db2.dll`, creates the service (account/password properties), recovery actions, starter config, ACLs, Event Log source. |
| `scripts/install-agent.ps1` | Elevated install/upgrade without MSI (uses `aleltagent.exe --install`), then runs verify. |
| `scripts/uninstall-agent.ps1` | Removes the service; `-Purge` also deletes config/spool/logs (warns on unacknowledged batches). |
| `scripts/verify-agent.ps1` | Service state/start mode/recovery, **no listening sockets** (`Get-NetTCPConnection`), Job Object membership, BELOW_NORMAL priority, config ACL + validity, spool state, log tail, recent Event Log errors. |

## Build the MSI

```bat
dotnet tool install --global wix
wix extension add -g WixToolset.Util.wixext
cd deploy\agent\wix
wix build Package.wxs -arch x64 -ext WixToolset.Util.wixext -d BinDir=..\..\..\edge-agent\core\build\x64-release\bin -o ALELTAgent.msi
```

Code signing hook (release): sign binaries before `wix build`, then the MSI:
`signtool sign /fd SHA256 /tr <rfc3161 url> /td SHA256 /sha1 <code-signing cert> aleltagent.exe aleltsrc_db2.dll ALELTAgent.msi`.

## Install / upgrade / uninstall

```bat
msiexec /i ALELTAgent.msi /qn /l*v aleltagent-msi.log HUBURL=https://hub:8443 AGENTID=edge-01 ^
  CONNECTIONID=erp DATABASE=<DATABASE> CERTTHUMBPRINT=<sha1> HUBPIN=<sha256> SPOOLDIR=E:\ALELT\spool ^
  SERVICEACCOUNT=CORP\svc-alelt$
sc start ALELTAgent
powershell -File "C:\Program Files\ALELT\Agent\verify-agent.ps1"
msiexec /x ALELTAgent.msi /qn          :: keeps %ProgramData%\ALELT\Agent (config, spool, positions)
```

Upgrades (`MajorUpgrade`) replace binaries only; config, `devtoken.bin`, spool batches and
`state.json` (durable/next positions) are outside the MSI and survive.
Full procedure, DBA grants and volume rules: `runbooks/agent-install.md`.
