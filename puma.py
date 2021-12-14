import json

from string import Template
from scrapy.spiders import CrawlSpider, Request
from scrapy import Selector

from ..items import Item


class ProductParser:
    currency = 'CNY'
    seen_ids = set()
    genders = {
        '男大童': 'Big Boy',
        '男女': 'Unisex Adults',
        '女': 'women',
        '男': 'men',
        '儿童': 'kid'
    }

    care_terms = ['棉', '聚酯纤维', '氨纶', '纤维', '皮革', '织物', '人造革']

    def parse_product(self, response):
        product_id = self.product_id(response)

        if not product_id or product_id in self.seen_ids:
            return

        self.seen_ids.add(product_id)
        raw_response = json.loads(response.body)

        if not raw_response.get('data') or not raw_response['data']['itemDetailList']:
            return

        item = Item()
        item['retailer_sku'] = product_id
        item['category'] = self.product_category(response)
        item['trail'] = self.product_trail(response)
        item['url'] = self.product_url(response)
        item['brand'] = self.product_brand()
        item['name'] = self.product_name(raw_response)
        item['gender'] = self.product_gender(raw_response)
        item['price'] = self.product_price(product_id, raw_response)
        item['currency'] = self.currency
        item['description'] = self.product_description(raw_response)
        item['care'] = self.product_care(raw_response)
        item['image_urls'] = self.product_img_urls(raw_response)
        item['skus'] = self.skus(raw_response)

        return item

    def product_id(self, response):
        prod_url = self.product_url(response)
        return prod_url.split('/')[-2:-1][0]

    def product_gender(self, raw_response):
        name = self.product_name(raw_response)
        gender = [g for g in self.genders.keys() if g in name]

        return self.genders[gender[0]] if gender else 'Unisex Adults'

    def product_category(self, response):
        return response.meta.get('trail')[0][0]

    def product_brand(self):
        return 'PUMA'

    def product_url(self, response):
        return response.meta.get('url')

    def product_name(self, raw_response):
        return raw_response['data']['itemDetailList'][0]['title']

    def product_description(self, raw_response):
        raw_desc = self.raw_description(raw_response)

        return [row for row in raw_desc if not any(term in row for term in self.care_terms)] if raw_desc else []

    def raw_description(self, raw_response):
        raw_desc = raw_response['data']['itemDetailList'][0]['description']

        if raw_desc:
            sel = Selector(text=raw_desc)
            return [row for row in sel.css('::text').getall() if row.strip()]

        return []

    def product_care(self, raw_response):
        raw_desc = self.raw_description(raw_response)

        return [row for row in raw_desc if any(term in row for term in self.care_terms)] if raw_desc else []

    def product_img_urls(self, raw_response):
        imgs = []

        for prod in raw_response['data']['itemDetailList']:
            if prod['attrSaleList'][0]['attributeValueList'][0]['itemAttributeValueImageList']:
                imgs.extend([img['picUrl'] for img in
                             prod['attrSaleList'][0]['attributeValueList'][0]['itemAttributeValueImageList']])

            elif prod['itemImageList'] and prod['itemImageList'][0]:
                imgs.append(prod['itemImageList'][0]['picUrl'])

        return imgs

    def product_trail(self, response):
        return response.meta.get('trail', 'None')

    def product_price(self, product_id, raw_response):
        price = [row['salePrice'] for row in raw_response['data']['itemDetailList'] if product_id == row['code']]

        return price[0] if price else None

    def product_common_detail(self, prod):
        return {'price': prod['salePrice'], 'currency': self.currency}

    def skus(self, raw_response):
        skus = []

        for prod in raw_response['data']['itemDetailList']:
            common_sku = self.product_common_detail(prod)

            for raw_sku in prod['skuList']:
                sku = common_sku.copy()
                sku["colour"] = raw_sku['attrSaleList'][0]['attributeValueList'][0]['attributeValueFrontName']
                sku['size'] = raw_sku['attrSaleList'][1]['attributeValueList'][0]['attributeValueFrontName']
                sku['out_of_stock'] = raw_sku['netqty'] <= 0
                sku_id = f'{sku["colour"]}_{sku["size"]}'
                skus.append({sku_id: sku})

        return skus


class PumaSpider(CrawlSpider):
    name = 'puma'
    allowed_domains = ['puma.com']
    start_urls = ['https://cn.puma.com/']
    PAGE_SIZE = 36
    api_url = 'https://cn.puma.com/pumacn/product/get/item/list/by/conditions.do'
    product_parser = ProductParser()
    base_url_t = "https://cn.puma.com{}"
    prod_base_url_t = "https://cn.puma.com/pdp/{}/{}.html"
    header = {'Content-Type': 'application/json;charset=UTF-8'}

    cat_payload = Template(
        """{"data": {"conditionList": [{"key": "type", "value": ["0"], "valueType": "basic"},
        {"key": "saleStatus", "value": [1], "valueType": "list"},
        {"key": "parentCategoryCode", "value": [$parent_code], "valueType": "list"}],
        "itemSortList": [{"frontName": "", "name": "list_time", "sort": 1}],
        "notIncludeSpuCodeList": [], "storeCode": "1811147124", "channelCode": 100},"page": $page, "size": $size}"""
    )

    prod_payload = Template(
        """{"data": {"conditionList": [{"style": $prod_id, "saleStatus": "1", "type": "0"}],
        "notIncludeSpuCodeList": [], "storeCode": "1811147124", "channelCode": 100}, "page": 1, "size": 100}"""
    )

    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36'}

    def parse_start_url(self, response):
        pattern = r'.*urlRename\":\[(.*)\],\"home.*'
        category_urls = response.css('script:contains("window.__INITIAL_STATE__")').re_first(pattern)
        url = 'https://cn.puma.com/pumacn/product/list/searchProductByCondition.do'

        return [Request(url=url, callback=self.parse_products, method='POST', headers=self.header,
                        body=self.cat_payload.substitute({'parent_code': cat_url['classify'], 'page': 1,
                                                          'size': self.PAGE_SIZE}),
                        meta={'id': cat_url['classify'], 'trail': [[cat_url['goalurl'].split('/')[1:],
                                                                    self.base_url_t.format(cat_url['goalurl'])]]})
                for cat_url in eval(category_urls) if cat_url['classify'] and cat_url['goalurl']]

    def parse_products(self, response):
        json_obj = json.loads(response.body)

        if not json_obj['code'] == '0' or not json_obj['data']['productList']:
            return

        for product in json_obj['data']['productList']:
            prod_id = product['spuCode']
            prod_ext = product['skuList'][0]['code']
            prod_url = self.prod_base_url_t.format(prod_id, prod_ext)

            yield Request(url=self.api_url, callback=self.product_parser.parse_product, method='POST',
                          headers=self.header, body=self.prod_payload.substitute({'prod_id': prod_id[:-2]}),
                          meta={'trail': response.meta['trail'].copy(), 'url': prod_url})

        yield from self.parse_pagination(response)

    def parse_pagination(self, response):
        payload = json.loads(response.request.body)
        payload['page'] = payload['page'] + 1
        response.meta['trail'] += [[payload['page']]]

        return [Request(url=response.url, callback=self.parse_products, method='POST',
                        headers=self.header,
                        body=json.dumps(payload), meta={'trail': response.meta['trail'].copy()})]
