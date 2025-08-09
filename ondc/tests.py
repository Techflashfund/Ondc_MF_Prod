{
    "context": {
        "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
        "domain": "ONDC:FIS14",
        "timestamp": "2025-08-09T05:28:21.532Z",
        "bap_id": "investment.flashfund.in",
        "bap_uri": "https://investment.flashfund.in/ondc",
        "transaction_id": "41bc1504-60e1-4eb0-a438-728f8d0d9096",
        "message_id": "975d31e5-4f8c-475a-b878-b38eefda950e",
        "version": "2.0.0",
        "ttl": "PT10M",
        "bpp_id": "api.cybrilla.com",
        "bpp_uri": "https://api.cybrilla.com/ondc",
        "action": "select",
    },
    "message": {
        "order": {
            "provider": {"id": "32"},
            "items": [
                {
                    "id": "22544",
                    "quantity": {
                        "selected": {"measure": {"value": "100", "unit": "INR"}}
                    },
                    "fulfillment_ids": ["101679"],
                }
            ],
            "fulfillments": [
                {
                    "id": "101679",
                    "type": "LUMPSUM",
                    "customer": {"person": {"id": "pan:OEXPS2710l"}},
                    "agent": {
                        "person": {"id": "Euin:E588669"},
                        "organization": {
                            "creds": [{"id": "ARN-310537", "type": "ARN"}]
                        },
                    },
                }
            ],
            "tags": [
                {
                    "display": False,
                    "descriptor": {
                        "name": "BAP Terms of Engagement",
                        "code": "BAP_TERMS",
                    },
                    "list": [
                        {
                            "descriptor": {
                                "name": "Static Terms (Transaction Level)",
                                "code": "STATIC_TERMS",
                            },
                            "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                        },
                        {
                            "descriptor": {
                                "name": "Offline Contract",
                                "code": "OFFLINE_CONTRACT",
                            },
                            "value": "true",
                        },
                    ],
                }
            ],
        }
    },
}
