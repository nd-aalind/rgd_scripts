UPDATE udm_staging.problemlist_rgd_v2 pl
LEFT JOIN semantics.comorbidity_llm_map cm 
    ON pl.problem_desc LIKE cm.problem_desc

SET
    pl.charlson_comorbidity   = cm.Charlson,
    pl.elixhauser_comorbidity = cm.Elixhauser

WHERE pl.problem_desc IS NOT NULL 
	AND (charlson_comorbidity IS NULL OR elixhauser_comorbidity IS NULL);