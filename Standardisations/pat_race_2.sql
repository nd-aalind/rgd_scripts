UPDATE rgd_udm_silver.patients
SET 
    pat_race_code_std = CASE
        WHEN LOWER(pat_race) IN (
            'white','caucasian','caucasian/white','wwhite','whtie','wnite','whiet','whiite',
            'whitte','whtte','whjite','wjhite','wjote','wgite','whgite','whiteq','whte',
            'hungarian','moore','white/unsure','white,english','white,declined to specify',
            'white,english,declined to specify','english,declined to specify',
            'white,other race','european,english,cherokee,italian,polish'
        ) THEN '2106-3'
        WHEN LOWER(pat_race) IN (
            'black or african american','african american','african america',
            'afrcan american','african americian','african amercian',
            'african american/black','african american & caucasian','somalia',
            'black/white','black & hispanic','black/asian','black and sicilian',
            'black or african american (biracial)','black/white/indian',
            'black,declined to specify','black,other race',
            'black or african american,native hawaiian or other pacific islander',
            'black or african american,white','black or african american,white,black',
            'black or african american,declined to specify','african american,white',
            'african american,other race','african american,declined to specify',
            'african american,black'
        ) THEN '2054-5'
        WHEN LOWER(pat_race) IN (
            'american indian or alaska nati','american indian or alaskan native',
            'native american indian','native american','indian',
            'white/american indian','white/black/american indian','native/white',
            'white,american indian or alaska native','white/spanish american indian',
            'white,spanish american indian'
        ) THEN '1002-5'
        WHEN LOWER(pat_race) IN (
            'east indian','asian/indian','sikh','usikh',
            'white/asian','asian/white','white,asian',
            'white,american indian or alaska native,asian'
        ) THEN '2028-9'
        WHEN LOWER(pat_race) IN ('pacific islander') THEN '2076-8'
        WHEN LOWER(pat_race) IN (
            'arabic','arab-palestinan','white/arabic','middle eastern',
            'other race~arabic','other race/pakistani',
            'other race/turkish','other race/hindu'
        ) THEN '2118-8'
        WHEN LOWER(pat_race) IN (
            'hispanic','hispanic-puerto rican','hispanic/white','white/hispanic',
            'white/puerto rican','white & puerto rican','latina',
            'puerto rican','white/black','other race,declined to specify'
        ) THEN '2131-1'
        WHEN LOWER(pat_race) IN (
            'unreported/refused to report','declined to specify','patient declined',
            'unspecified','state prohibited','unknown','unkown','uknown','unknow',
            'unkonown','unknownc','declined','none-other','n/a',
            'na@dent.com','donotemail@dent.com','20181227@dentinstitue.com',
            'kath_lean1@yahoo.com','mrschowski@aol.com','ekwilos@hotmail.com',
            'e','w','h','o','u','c','r','osco'
        ) THEN 'UNK'

        ELSE 'Flagged'
    END,
    pat_race_std = CASE
        WHEN LOWER(pat_race) IN (
            'white','caucasian','caucasian/white','wwhite','whtie','wnite','whiet','whiite',
            'whitte','whtte','whjite','wjhite','wjote','wgite','whgite','whiteq','whte',
            'hungarian','moore','white/unsure','white,english','white,declined to specify',
            'white,english,declined to specify','english,declined to specify',
            'white,other race','european,english,cherokee,italian,polish'
        ) THEN 'White'
        WHEN LOWER(pat_race) IN (
            'black or african american','african american','african america',
            'afrcan american','african americian','african amercian',
            'african american/black','african american & caucasian','somalia',
            'black/white','black & hispanic','black/asian','black and sicilian',
            'black or african american (biracial)','black/white/indian',
            'black,declined to specify','black,other race',
            'black or african american,native hawaiian or other pacific islander',
            'black or african american,white','black or african american,white,black',
            'black or african american,declined to specify','african american,white',
            'african american,other race','african american,declined to specify',
            'african american,black'
        ) THEN 'Black or African American'
        WHEN LOWER(pat_race) IN (
            'american indian or alaska nati','american indian or alaskan native',
            'native american indian','native american','indian',
            'white/american indian','white/black/american indian','native/white',
            'white,american indian or alaska native','white/spanish american indian',
            'white,spanish american indian'
        ) THEN 'American Indian or Alaska Native'
        WHEN LOWER(pat_race) IN (
            'east indian','asian/indian','sikh','usikh',
            'white/asian','asian/white','white,asian',
            'white,american indian or alaska native,asian'
        ) THEN 'Asian'
        WHEN LOWER(pat_race) IN ('pacific islander')
        THEN 'Native Hawaiian or Other Pacific Islander'
        WHEN LOWER(pat_race) IN (
            'arabic','arab-palestinan','white/arabic','middle eastern',
            'other race~arabic','other race/pakistani',
            'other race/turkish','other race/hindu'
        ) THEN 'Middle Eastern or North African'
        WHEN LOWER(pat_race) IN (
            'hispanic','hispanic-puerto rican','hispanic/white','white/hispanic',
            'white/puerto rican','white & puerto rican','latina',
            'puerto rican','white/black','other race,declined to specify'
        ) THEN 'Other Race'
        WHEN LOWER(pat_race) IN (
            'unreported/refused to report','declined to specify','patient declined',
            'unspecified','state prohibited','unknown','unkown','uknown','unknow',
            'unkonown','unknownc','declined','none-other','n/a',
            'na@dent.com','donotemail@dent.com','20181227@dentinstitue.com',
            'kath_lean1@yahoo.com','mrschowski@aol.com','ekwilos@hotmail.com',
            'e','w','h','o','u','c','r','osco'
        ) THEN 'UNK'

        ELSE 'Flagged'
    END
WHERE pat_race_code_std = 'Flagged'
   OR pat_race_std = 'Flagged';
