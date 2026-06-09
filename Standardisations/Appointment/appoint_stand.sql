SELECT DISTINCT
    appointment_name,

    CASE
        /* NULL / blank */
        WHEN appointment_name IS NULL
             OR TRIM(appointment_name) = ''
        THEN NULL

        /* Numeric only */
        WHEN TRIM(appointment_name) REGEXP '^[0-9]+(\\.[0-9]+)?$'
        THEN 'Other'

        ELSE
            COALESCE(

                NULLIF(

                    TRIM(

                        REGEXP_REPLACE(

                            REGEXP_REPLACE(

                                REGEXP_REPLACE(

                                    REGEXP_REPLACE(

                                        REGEXP_REPLACE(

                                            REGEXP_REPLACE(

                                                REGEXP_REPLACE(

                                                    REGEXP_REPLACE(

                                                        REGEXP_REPLACE(

                                                            REGEXP_REPLACE(

                                                                REGEXP_REPLACE(

                                                                    REGEXP_REPLACE(

                                                                        REGEXP_REPLACE(

                                                                            REGEXP_REPLACE(

                                                                                /* Step 1: Remove fractions like 1/2 */
                                                                                appointment_name,
                                                                                '[0-9]+/[0-9]+',
                                                                                ''
                                                                            ),

                                                                            /* Step 2: Remove "1.5 hr or 2 hrs" */
                                                                            '(^|[^A-Za-z])[0-9]+\\.?[0-9]*\\s*(hours|hour|hrs|hr|h)\\s*or\\s*[0-9]+\\.?[0-9]*\\s*(hours|hour|hrs|hr|h)([^A-Za-z]|$)',
                                                                            ''
                                                                        ),

                                                                        /* Step 3: Remove leading durations */
                                                                        '^[0-9]+\\s*(MINUTE|MINUTES|MIN|MINS|HOUR|HOURS|HR|HRS)\\s+',
                                                                        ''
                                                                    ),

                                                                    /* Step 4: Remove embedded durations */
                                                                    '(^|[^A-Za-z])[0-9]+\\.?[0-9]*\\s*(hours|hour|mins|minutes|hrs|hr|min|h)([^A-Za-z]|$)',
                                                                    ''
                                                                ),

                                                                /* Step 5: Remove day/week/month/year durations */
                                                                '[0-9]+\\.?[0-9]*\\s*(days|day|weeks|week|months|month|years|year)',
                                                                ''
                                                            ),

                                                            /* Step 6: Remove parenthetical content */
                                                            '\\([^)]*\\)',
                                                            ''
                                                        ),

                                                        /* Step 7: Remove trailing "-180" */
                                                        '\\s*[-/]\\s*[0-9]+[\\s-]*$',
                                                        ''
                                                    ),

                                                    /* Step 8: Remove trailing "_60" or " 75" */
                                                    '[_\\s]+[0-9]+\\s*$',
                                                    ''
                                                ),

                                                /* Step 9: Remove leading numbers except 70+ */
                                                '^[0-9\\.]+\\s+(?!\\+)',
                                                ''
                                            ),

                                            /* Step 10: Replace commas */
                                            ',',
                                            ' '
                                        ),

                                        /* Step 11: Remove trailing hyphens */
                                        '\\s*-+\\s*$',
                                        ''
                                    ),

                                    /* Step 12: Collapse multiple spaces */
                                    '\\s+',
                                    ' '
                                ),

                                /* Step 13: Remove leading asterisks */
                                '^\\*+\\s*',
                                ''
                            ),

                            /* Step 14: Remove trailing * . _ */
                            '[*._]+\\s*$',
                            ''
                        )

                    ),

                    ''
                ),

                /* Final fallback */
                'Other'
            )

    END AS appointment_name_std

FROM udm_staging.appointment_fn;