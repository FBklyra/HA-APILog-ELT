/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ALELT_VERSION_H
#define ALELT_VERSION_H

#define ALELT_AGENT_VERSION "0.1.0"
#define ALELT_AGENT_VERSION_RC 0, 1, 0, 0
#define ALELT_PROTOCOL_VERSION 1
#define ALELT_SERVICE_NAME_W L"ALELTAgent"
#define ALELT_SERVICE_DISPLAY_W L"ALELT Agent"
#define ALELT_SERVICE_DESC_W                                                                                         \
    L"ALELT CDC edge agent: reads the database log read-only and ships raw batches to the ALELT hub " \
    L"over outbound HTTPS. No listening ports."

#endif
