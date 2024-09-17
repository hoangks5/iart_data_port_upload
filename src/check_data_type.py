from dateutil import parser



def check_data_type(df, region):
    wrong_data_type = []
    date_time_dict = df.to_dict(orient='list')
    if date_time_dict == {}:
        return []
    if region == 'au':
        for index, date in enumerate(date_time_dict['date/time']):
            try:
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'date/time', "row": 9 + index, "value": date })
        key_chekc_isnumric = ['settlement id','order postal','product sales','shipping credits','gift wrap credits','promotional rebates','sales tax collected','low value goods','selling fees','fba fees','other transaction fees','other','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace(',','').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'ca':
        for index, date in enumerate(date_time_dict['date/time']):
            try:
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'date/time', "row": 9 + index, "value": date })
        key_chekc_isnumric = ['settlement id','product sales','product sales tax','shipping credits','shipping credits tax','gift wrap credits','giftwrap credits tax','Regulatory fee','Tax on regulatory fee','promotional rebates','promotional rebates tax','marketplace withheld tax','selling fees','fba fees','other transaction fees','other','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace(',','').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'de':
        for index, date in enumerate(date_time_dict['Datum/Uhrzeit']):
            try:
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'Datum/Uhrzeit', "row": 9 + index, "value": date })
        
        key_chekc_isnumric = ['Abrechnungsnummer','Umsätze','Produktumsatzsteuer','Gutschrift für Versandkosten','Steuer auf Versandgutschrift','Gutschrift für Geschenkverpackung','Steuer auf Geschenkverpackungsgutschriften','Rabatte aus Werbeaktionen','Steuer auf Aktionsrabatte','Einbehaltene Steuer auf Marketplace','Verkaufsgebühren','Gebühren zu Versand durch Amazon','Andere Transaktionsgebühren','Andere','Gesamt']
    
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace('.','').replace(',','.').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'es':
        for index, date in enumerate(date_time_dict['fecha y hora']):
            try:
                moth_translate = {
                    'ene': 'Jan',
                    'feb': 'Feb',
                    'mar': 'Mar',
                    'abr': 'Apr',
                    'may': 'May',
                    'jun': 'Jun',
                    'jul': 'Jul',
                    'ago': 'Aug',
                    'sep': 'Sep',
                    'oct': 'Oct',
                    'nov': 'Nov',
                    'dic': 'Dec'
                }
                for key in moth_translate:
                    date = date.replace(key, moth_translate[key])
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'fecha y hora', "row": 9 + index, "value": date })
        key_chekc_isnumric = ['identificador de pago','ventas de productos','impuesto de ventas de productos','abonos de envío','impuestos por abonos de envío','abonos de envoltorio para regalo','giftwrap credits tax','devoluciones promocionales','promotional rebates tax','impuesto retenido en el sitio web','tarifas de venta','tarifas de Logística de Amazon','tarifas de otras transacciones','otro','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace('.','').replace(',','.').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'fr':
        for index, date in enumerate(date_time_dict['date/heure']):
            try:
                moth_translate = {
                    'janv': 'Jan',
                    'févr': 'Feb',
                    'mars': 'Mar',
                    'avr': 'Apr',
                    'mai': 'May',
                    'juin': 'Jun',
                    'juil': 'Jul',
                    'août': 'Aug',
                    'sept': 'Sep',
                    'oct': 'Oct',
                    'nov': 'Nov',
                    'déc': 'Dec'
                }
                for key in moth_translate:
                    date = date.replace(key, moth_translate[key])
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'date/heure', "row": 9 + index, "value": date })
        key_chekc_isnumric = ['numéro de versement','ventes de produits','Taxes sur la vente des produits',"crédits d'expédition","taxe sur les crédits d’expédition","crédits sur l'emballage cadeau","Taxes sur les crédits cadeaux","Rabais promotionnels","Taxes sur les remises promotionnelles","Taxes retenues sur le site de vente","frais de vente","Frais Expédié par Amazon","autres frais de transaction","autre","total"]
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace('.','').replace(',','.').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'it':
        for index, date in enumerate(date_time_dict['Data/Ora:']):
            try:
                moth_translate = {
                    'gen': 'Jan',
                    'feb': 'Feb',
                    'mar': 'Mar',
                    'apr': 'Apr',
                    'mag': 'May',
                    'giu': 'Jun',
                    'lug': 'Jul',
                    'ago': 'Aug',
                    'set': 'Sep',
                    'ott': 'Oct',
                    'nov': 'Nov',
                    'dic': 'Dec'
                }
                for key in moth_translate:
                    date = date.replace(key, moth_translate[key])
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'Data/Ora:', "row": 9 + index, "value": date })
        key_chekc_isnumric = ["Numero pagamento","Vendite","imposta sulle vendite dei prodotti","Accrediti per le spedizioni","imposta accrediti per le spedizioni","Accrediti per confezioni regalo","imposta sui crediti confezione regalo","Sconti promozionali","imposta sugli sconti promozionali","trattenuta IVA del marketplace","Commissioni di vendita","Costi del servizio Logistica di Amazon","Altri costi relativi alle transazioni","Altro","totale"]
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace('.','').replace(',','.').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'jp':
        for index, date in enumerate(date_time_dict['日付/時間']):
            try:
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": '日付/時間', "row": 9 + index, "value": date })
        key_chekc_isnumric = ['決済番号','商品売上','商品の売上税','配送料','配送料の税金','ギフト包装手数料','ギフト包装クレジットの税金','Amazonポイントの費用','プロモーション割引額','プロモーション割引の税金','源泉徴収税を伴うマーケットプレイス','手数料','FBA 手数料','トランザクションに関するその他の手数料','その他','合計']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace(',','').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'mx':
        for index, date in enumerate(date_time_dict['fecha/hora']):
            try:
                moth_translate = {
                    'ene': 'Jan',
                    'feb': 'Feb',
                    'mar': 'Mar',
                    'abr': 'Apr',
                    'may': 'May',
                    'jun': 'Jun',
                    'jul': 'Jul',
                    'ago': 'Aug',
                    'sep': 'Sep',
                    'oct': 'Oct',
                    'nov': 'Nov',
                    'dic': 'Dec'
                }
                for key in moth_translate:
                    date = date.replace(key, moth_translate[key])
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'fecha/hora', "row": 9 + index, "value": date })
        key_chekc_isnumric = ["Id. de liquidación",'ventas de productos','impuesto de ventas de productos','créditos de envío','impuesto de abono de envío','créditos por envoltorio de regalo','impuesto de créditos de envoltura','Tarifa reglamentaria','Impuesto sobre tarifa reglamentaria','descuentos promocionales','impuesto de reembolsos promocionales','impuesto de retenciones en la plataforma','tarifas de venta','tarifas fba','tarifas de otra transacción','otro','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace('.','').replace(',','.').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'nl':
        for index, date in enumerate(date_time_dict['datum/tijd']):
            try:
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'datum/tijd', "row": 9 + index, "value": date })
        key_chekc_isnumric = ['schikkings-ID','verkoop van producten','Verzendtegoeden','kredietpunten cadeauverpakking','promotiekortingen','geïnde omzetbelasting','Belasting voor marketplace-facilitator','verkoopkosten','fba-vergoedingen','overige transactiekosten','overige','totaal']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace('.','').replace(',','.').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'uk':
        for index, date in enumerate(date_time_dict['date/time']):
            try:
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'date/time', "row": 9 + index, "value": date })
        key_chekc_isnumric = ['settlement id','product sales tax','postage credits','shipping credits tax','gift wrap credits','giftwrap credits tax','promotional rebates','promotional rebates tax','marketplace withheld tax','selling fees','fba fees','other transaction fees','other','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace(',','').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
    elif region == 'us':
        for index, date in enumerate(date_time_dict['date/time']):
            try:
                parser.parse(date)
            except:
                wrong_data_type.append({ "column": 'date/time', "row": 9 + index, "value": date })
        key_chekc_isnumric = ['settlement id','product sales','product sales tax','shipping credits','shipping credits tax','gift wrap credits','giftwrap credits tax','Regulatory Fee','Tax On Regulatory Fee','promotional rebates','promotional rebates tax','marketplace withheld tax','selling fees','fba fees','other transaction fees','other','total']
        for key in key_chekc_isnumric:
            for index, value in enumerate(date_time_dict[key]):
                try:
                    float(str(value).replace('.','').replace(',','.').replace(' ',''))
                except:
                    wrong_data_type.append({ "column": key, "row": 9 + index, "value": value })
                    
    return wrong_data_type