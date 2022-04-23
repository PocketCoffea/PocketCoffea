JECversions = {
    '2016_PreVFP' : {
        'MC' : 'Summer19UL16APV_V7_MC',
        'Data' : {
            'B' : 'Summer19UL16APV_RunBCD_V7_DATA',
            'C' : 'Summer19UL16APV_RunBCD_V7_DATA',
            'D' : 'Summer19UL16APV_RunBCD_V7_DATA',
            'E' : 'Summer19UL16APV_RunEF_V7_DATA',
            'F' : 'Summer19UL16APV_RunEF_V7_DATA'
            }
        },
    '2016_PostVFP' : {
        'MC' : 'Summer19UL16_V7_MC',
        'Data' : {
            'F' : 'Summer19UL16_RunFGH_V7_DATA',
            'G' : 'Summer19UL16_RunFGH_V7_DATA',
            'H' : 'Summer19UL16_RunFGH_V7_DATA'
            }
        },
    '2017' : {
        'MC' : 'Summer19UL17_V5_MC',
        'Data' : {
            'B' : 'Summer19UL17_RunB_V5_DATA',
            'C' : 'Summer19UL17_RunC_V5_DATA',
            'D' : 'Summer19UL17_RunD_V5_DATA',
            'E' : 'Summer19UL17_RunE_V5_DATA',
            'F' : 'Summer19UL17_RunF_V5_DATA'
            }
        },
    '2018' : {
        'MC' : 'Summer19UL18_V5_MC',
        'Data' : {
            'A' : 'Summer19UL18_RunA_V5_DATA',
            'B' : 'Summer19UL18_RunB_V5_DATA',
            'C' : 'Summer19UL18_RunC_V5_DATA',
            'D' : 'Summer19UL18_RunD_V5_DATA',
            }
        }
}

JECtarFiles = {
    '2016_PreVFP' : [
        'parameters/corrections/JEC/Summer19UL16APV_V7_MC.tar.gz',
        'parameters/corrections/JEC/Summer19UL16APV_RunBCD_V7_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL16APV_RunEF_V7_DATA.tar.gz',
    ],
    '2016_PostVFP' : [
        'parameters/corrections/JEC/Summer19UL16_V7_MC.tar.gz',
        'parameters/corrections/JEC/Summer19UL16_RunFGH_V7_DATA.tar.gz',
    ],
    '2017' : [
        'parameters/corrections/JEC/Summer19UL17_RunB_V5_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL17_RunC_V5_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL17_RunD_V5_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL17_RunE_V5_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL17_RunF_V5_DATA.tar.gz',
    ],
    '2018' : [
        'parameters/corrections/JEC/Summer19UL18_RunA_V5_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL18_RunB_V5_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL18_RunC_V5_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL18_RunD_V5_DATA.tar.gz',
        'parameters/corrections/JEC/Summer19UL18_V5_MC.tar.gz',
    ]
}
