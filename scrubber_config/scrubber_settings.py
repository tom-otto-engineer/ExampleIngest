scrubber_config = {"rules": [
                                            {
                                                "field": "Selling Price",
                                                "rule_type": "Number",
                                                "params": {
                                                    "decimal_places": 0,
                                                    "fix_decimal_places": True,
                                                    "minimum_value": 0,
                                                    "maximum_value": 86400000,
                                                    "fallback_mode": "use_default",
                                                    "default_value": 0
                                                }
                                            },
                                            {
                                                "field": "Original Price",
                                                "rule_type": "Number",
                                                "params": {
                                                    "decimal_places": 0,
                                                    "fix_decimal_places": True,
                                                    "minimum_value": 0,
                                                    "maximum_value": 86400000,
                                                    "fallback_mode": "use_default",
                                                    "default_value": 0
                                                }
                                            },
                                            {
                                                "field": "Size",
                                                "rule_type": "Number",
                                                "params": {
                                                    "decimal_places": 0,
                                                    "fix_decimal_places": True,
                                                    "minimum_value": 0,
                                                    "maximum_value": 86400000,
                                                    "fallback_mode": "use_default",
                                                    "default_value": 0
                                                }
                                            },
                                            {
                                                "field": "Rating",
                                                "rule_type": "Number",
                                                "params": {
                                                    "decimal_places": 0,
                                                    "fix_decimal_places": True,
                                                    "minimum_value": 0,
                                                    "maximum_value": 86400000,
                                                    "fallback_mode": "use_default",
                                                    "default_value": 0
                                                }
                                            },
                                            {
                                                "field": "date",
                                                "rule_type": "Date",
                                                "params": {
                                                    "date_format": "%Y%m%d",
                                                    "range_check": "rolling",
                                                    "range_minimum": -43614,
                                                    "range_maximum": 30,
                                                    "fallback_mode": "do_not_replace"
                                                }
                                            }
                                        ]
                               }