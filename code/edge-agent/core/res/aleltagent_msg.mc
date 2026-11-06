; // ALELT agent Event Log messages. SPDX-License-Identifier: Apache-2.0
MessageIdTypedef=DWORD
SeverityNames=(Success=0x0:STATUS_SEVERITY_SUCCESS
               Informational=0x1:STATUS_SEVERITY_INFORMATIONAL
               Warning=0x2:STATUS_SEVERITY_WARNING
               Error=0x3:STATUS_SEVERITY_ERROR)
LanguageNames=(English=0x409:MSG00409)

MessageId=0x1
Severity=Informational
SymbolicName=MSG_ALELT_INFO
Language=English
%1
.

MessageId=0x2
Severity=Warning
SymbolicName=MSG_ALELT_WARN
Language=English
%1
.

MessageId=0x3
Severity=Error
SymbolicName=MSG_ALELT_ERROR
Language=English
%1
.
