
# Information taken from the NYSE TAQ client spec

class ByteSpec:
    '''
    A description of the records in TAQ file. Borrowed from @davclark
    '''
    qts_col_dt = [  ('Hour',                       'S2'),
                    ('Minute',                     'S2'),
                    ('Second',                     'S2'),
                    ('Milliseconds',               'S3'),
                    ('Exchange',                   'S1'),
                    ('Symbol_Root',                'S6'),
                    ('Symbol_Suffix',             'S10'),
                    ('Bid_Price',                 'S11'),
                    ('Bid_Size',                   'S7'),
                    ('Ask_Price',                 'S11'),
                    ('Ask_Size',                   'S7'),
                    ('Quote_Condition',            'S1'), 
                    ('Market_Maker',               'S4'),
                    ('Bid_Exchange',               'S1'),
                    ('Ask_Exchange',               'S1'),
                    ('Sequence_Number',           'S16'),
                    ('National_BBO_Ind',           'S1'),
                    ('NASDAQ_BBO_IND',             'S1'),
                    ('Quote_Cancel_Correction',    'S1'),
                    ('Source_of_Quote',            'S1'),
                    ('Retail_Interest_Ind',        'S1'),
                    ('Short_Sale_Restriction_Ind', 'S1'),
                    ('LULD_BBO_Ind_CQS',           'S1'),
                    ('LULD_BBO_Ind_UTP',           'S1'),
                    ('FINRA_ADF_MPID_Ind',         'S1'),
                    ('SIP_Generated_Message_ID',   'S1'),
                    ('National_BBO_LULD_Ind',      'S1'),
                    ('Line_Change',                'S2')  ]

    trd_col_dt = [  ('Hour',                       'S2'),
                    ('Minute',                     'S2'),
                    ('Second',                     'S2'),
                    ('Milliseconds',               'S3'),
                    ('Exchange',                   'S1'),
                    ('Symbol_Root',                'S6'),
                    ('Symbol_Suffix',             'S10'),
                    ('Sale_Condition',             'S4'),
                    ('Trade_Volume',               'S9'),
                    ('Trade_Price',               'S11'),
                    ('Trade_Stop_Ind',             'S1'),
                    ('Trade_Correction_Ind',       'S2'),
                    ('Trade_Sequence_No',         'S16'),
                    ('Source_of_Trade',            'S1'),
                    ('Trade_Facility',             'S1'),
                    ('Line_Change',                'S2')  ]
    bbo_col_dt = []
    mtr_col_dt = []

    qts_strings = [ 'Exchange',
                    'Symbol_Root',
                    'Symbol_Suffix',
                    'Quote_Condition',
                    'Market_Maker',
                    'Bid_Exchange',
                    'Ask_Exchange',
                    'National_BBO_Ind',
                    'Quote_Cancel_Correction',
                    'Source_of_Quote',
                    'Retail_Interest_Ind',
                    'Short_Sale_Restriction_Ind',
                    'LULD_BBO_Ind_CQS',
                    'LULD_BBO_Ind_UTP',
                    'FINRA_ADF_MPID_Ind',
                    'SIP_Generated_Message_ID',
                    'National_BBO_LULD_Ind' ]

    qts_numericals = [ 'Hour',
                       'Minute',
                       'Second',
                       'Milliseconds',
                       'Bid_Price',
                       'Bid_Size',
                       'Ask_Price',
                       'Ask_Size',
                       'Sequence_Number',
                       'NASDAQ_BBO_IND' ]

    trd_numericals = [ 'Hour',
                       'Minute',
                       'Second',
                       'Milliseconds',
                       'Trade_Volume',
                       'Trade_Price' ]

    trd_strings = [ 'Exchange',
                    'Symbol',
                    'Sale_Condition',
                    'Trade_Stop_Ind',
                    'Trade_Correction_Ind',
                    'Trade_Sequence_No',
                    'Source_of_Trade',
                    'Trade_Facility',
                    'Line_Change' ]

    dict = {'qts_strings': qts_strings, 'qts_numericals': qts_numericals, 'trd_strings': trd_strings,
            'trd_numericals': trd_numericals}