import pymapd
import pandas as pd

with pymapd.connect('omnisci://admin:HyperInteractive@localhost:6274/omnisci') as con:

    print(pd.read_sql("SELECT count(*) from omnisci_states;", con))

    print(pd.read_sql("""select count(*)
    from omnisci_states as s""", con))

    # vega='{"widget_id","compression_level","vega_json","nonce","client"} {"8702332822301651","3","{""width"":1002,""height"":726,""viewRenderOptions"":{""premultipliedAlpha"":false},""data"":[{""name"":""pointmap"",""sql"":""SELECT conv_4326_900913_x(st_xmin(omnisci_geo)) AS x, conv_4326_900913_y(st_ymin(omnisci_geo )) AS y FROM omnisci_states WHERE ((st_xmin(omnisci_geo) is not null\n          AND st_ymin(omnisci_geo ) is not null\n          AND st_xmin(omnisci_geo) >= -178.12315200000032 AND st_xmin(omnisci_geo) <= -67.26987899999968 AND st_ymin(omnisci_geo ) >= -0.8144879012842097 AND st_ymin(omnisci_geo ) <= 61.96302517868901)) LIMIT 10000000"",""enableHitTesting"":false}],""scales"":[{""name"":""x"",""type"":""linear"",""domain"":[-19828578.576412328,-7488448.674977641],""range"":""width""},{""name"":""y"",""type"":""linear"",""domain"":[-90671.43229163112,8850380.771762503],""range"":""height""},{""name"":""pointmap_fillColor"",""type"":""linear"",""domain"":[0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1],""range"":[""rgba(17,95,154,0.475)"",""rgba(25,132,197,0.5471153846153846)"",""rgba(34,167,240,0.6192307692307691)"",""rgba(72,181,196,0.6913461538461538)"",""rgba(118,198,143,0.7634615384615384)"",""rgba(166,215,91,0.835576923076923)"",""rgba(201,229,47,0.85)"",""rgba(208,238,17,0.85)"",""rgba(208,244,0,0.85)""],""accumulator"":""density"",""minDensityCnt"":""-2ndStdDev"",""maxDensityCnt"":""2ndStdDev"",""clamp"":true}],""projections"":[],""marks"":[{""type"":""symbol"",""from"":{""data"":""pointmap""},""properties"":{""xc"":{""scale"":""x"",""field"":""x""},""yc"":{""scale"":""y"",""field"":""y""},""fillColor"":{""scale"":""pointmap_fillColor"",""value"":0},""shape"":""circle"",""width"":5,""height"":5}}]}","11","http:10.109.0.9"}'
    # try:
    #     print(con.render_vega(vega))
    # except:
    #     # gpu not enabled
    #     # we want the error in the log
    #     pass


# Normalize timestamps in log file, so diff is minimized
with open('/omnisci-storage/data/mapd_log/omnisci_server.INFO') as src:
    # with open('/src/tests/gold/omnisci_server.INFO') as src:
    with open('/src/target/test/omnisci_server.INFO', 'w') as tgt:
        i = 0
        for line in src:
            s = line.split(' ', 3)
            if len(s) > 2 and len(s[0]) == 26:
                try:
                    pd.to_datetime(s[0])
                    i += 1
                    line = '2020-07-01T00:00:00.{:06} {} {} {}'.format(i, s[1], '16', s[3])
                except:
                    pass
            tgt.write(line)
