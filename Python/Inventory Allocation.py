## PYTHON 
## ******************************************************************** ##
## author: hw_wangzirui
## purpose: 排产 - 库存预分配
## create time: 2022/09/15 18:59:29 GMT+08:00
## 来源表：
## gf_dwi.xyg_gfoen_dwi_cust_parties
## gf_dwr_model.xyg_gfoen_dwr_iapa_item_storage
## gf_dwr_model.xyg_gfoen_dwr_iapa_cust_delivery
## gf_dwr_model.xyg_gfoen_dwr_iapa_order_progress
## gf_dwr_model.xyg_gfoen_dwr_iapa_organizations_id
## 目标表：
## xyg_gfoen_dm_iapa_model_output_inv_alloc_detail
## xyg_gfoen_dm_iapa_model_output_inv_alloc
## xyg_gfoen_dm_iapa_model_output_inv_alloc_order_plan
## xyg_gfoen_dwr_iapa_model_alloc_tmp
## ******************************************************************** ##
import numpy as np
import pandas as pd
import warnings
import psycopg2
from sqlalchemy import create_engine
from urllib import parse
import base64
from Crypto.Cipher import AES
import configparser
warnings.filterwarnings("ignore")

class InventoryAllocation():
    '''库存预分配模型'''
    def __init__(self, plan_number, days, config):
        # self.dim_sql_parent = "select * from gf_dwr_model.xyg_gfoen_dwr_iapa_company_shortname_parent" # 子母公司
        self.dim_sql_parent = "select party_name, party_short_name, parent from T1 where parent is not null and party_short_name is not null" # 子母公司
        self.dim_sql_storage = "select * from T2" # 库存
        self.dim_sql_plan = "select * from T3" # 交货计划
        self.dim_sql_order = "select * from T4" # 生产进度完成表
        self.dim_sql_organization = "select organization_code, organization_id from T5"
        # self.plan_number = 'WH20230307001'
        self.plan_number = plan_number # 传入的排产单号
        # self.days = 3 # 传入的天数
        self.days = days # 传入的天数
        self.key = config['model']['key']
        self.iv = config['model']['iv']
        self.host = config['model']['host']
        self.database = config['model']['database']
        self.user = config['model']['user']
        self.password = config['model']['password']


    # 解密过程逆着加密过程写
    def AES_de(self, data, key, iv):
        # 将密文字符串重新编码成二进制形式
        data = data.encode("utf-8")
        # 将base64的编码解开
        data = base64.b64decode(data)
        # 创建解密对象
        AES_de_obj = AES.new(self.key.encode("utf-8"), AES.MODE_CBC, self.iv.encode("utf-8"))
        # 完成解密
        AES_de_str = AES_de_obj.decrypt(data)
        # 去掉补上的空格
        AES_de_str = AES_de_str.strip()
        # 对明文解码
        AES_de_str = AES_de_str.decode("utf-8")
        return AES_de_str


    def dwsConnect(self, host, database, user, password):# 连接数据库
        '''通过psycopg2包连接数据库，接入模型所用中间表'''
        con = None
        # 创建psycopg2连接，输入连接信息
        try:
            con = psycopg2.connect(
                host = host,
                database = database,
                user = user,
                password = password,
                port = 8000
            )
            con.set_client_encoding('utf8') #设置client编码格式为utf8来解析中文
            cur = con.cursor()
            cur.execute(self.dim_sql_parent) #cursor传递sql语句
            version_parent = cur.fetchall() #cursor执行sql语句并返回查询结果
            cur = con.cursor()
            cur.execute(self.dim_sql_storage) #cursor传递sql语句
            version_storage = cur.fetchall() #cursor执行sql语句并返回查询结果
            cur = con.cursor()
            cur.execute(self.dim_sql_plan) #cursor传递sql语句
            version_plan = cur.fetchall() #cursor执行sql语句并返回查询结果
            cur = con.cursor()
            cur.execute(self.dim_sql_order) #cursor传递sql语句
            version_order = cur.fetchall() #cursor执行sql语句并返回查询结果
            cur = con.cursor()
            cur.execute(self.dim_sql_organization)
            version_organization = cur.fetchall()
        except psycopg2.DatabaseError as e: #打印错误
            print('Error:' + e)
            sys.exit(1)
        
        finally: #关闭连接
            if con:
                con.close()
                
            # 子母公司表创建
            df_parent = pd.DataFrame(version_parent)
            df_parent.columns = ['customer_name', 'customer_short_name', 'customer_parent']
            
            
            # 库存表创建
            df_storage = pd.DataFrame(version_storage)
            df_storage.columns = ['stat_date', 'organization_id', 'inventory_item_id', 'item_number', 'height', 'width', 'thickness', 'product_type', 'package_type', 'package_pcs', 'drawing_number', 'available_inventory', 'available_inventory_ton', 'attribute15', 'dws_create_date', 'dws_update_date']
            df_storage[['organization_id', 'inventory_item_id', 'height', 'width', 'thickness', 'package_pcs', 'available_inventory', 'available_inventory_ton']] = df_storage[['organization_id', 'inventory_item_id', 'height', 'width', 'thickness', 'package_pcs', 'available_inventory', 'available_inventory_ton']].apply(pd.to_numeric) # 转换数据类型，将默认的字符串转换成数值型
            
            # 交货计划表创建
            df_plan = pd.DataFrame(version_plan)
            df_plan.columns = ['cust_delivery_id', 'customer_id', 'customer_name', 'organization_id', 'delivery_location', 'inventory_item_id', 'item_number', 'thickness', 'glass_type', 'height', 'width', 'product_type', 'package_type', 'package_pcs', 'drawing_number', 'delivery_plan_day_1',  'delivery_plan_day_2', 'delivery_plan_day_3', 'delivery_plan_day_4', 'delivery_plan_day_5', 'delivery_plan_3_day_total', 'delivery_plan_total', 'transfer_ratio', 'dws_create_date', 'dws_update_date']
            df_plan[['customer_id', 'organization_id', 'inventory_item_id', 'thickness', 'height', 'width', 'package_pcs', 'delivery_plan_day_1', 'delivery_plan_day_2', 'delivery_plan_day_3', 'delivery_plan_day_4', 'delivery_plan_day_5', 'delivery_plan_3_day_total', 'delivery_plan_total', 'transfer_ratio']] = df_plan[['customer_id', 'organization_id',  'inventory_item_id', 'thickness', 'height', 'width', 'package_pcs', 'delivery_plan_day_1', 'delivery_plan_day_2', 'delivery_plan_day_3', 'delivery_plan_day_4', 'delivery_plan_day_5', 'delivery_plan_3_day_total', 'delivery_plan_total', 'transfer_ratio']].apply(pd.to_numeric)
            
            
            # 订单表创建
            df_order = pd.DataFrame(version_order)
            df_order.columns = ['customer_id', 'customer_name', 'organization_id', 'inventory_item_id', 'item_number', 'order_date', 'order_number', 'order_id', 'order_demand_number', 'order_demand_id', 'order_requirement_quantity', 'order_incomplete_quantity', 'height', 'width', 'thickness', 'product_type', 'package_type', 'package_pcs', 'drawing_number', 'remarks', 'row_number', 'dws_create_date', 'dws_update_date']
            df_order.drop(columns='row_number', inplace=True) # 删除无效字段
            df_order[['organization_id', 'inventory_item_id', 'order_id', 'order_demand_id',  'order_requirement_quantity', 'order_incomplete_quantity', 'height', 'width', 'thickness', 'package_pcs']] = df_order[['organization_id', 'inventory_item_id', 'order_id', 'order_demand_id',  'order_requirement_quantity', 'order_incomplete_quantity', 'height', 'width', 'thickness', 'package_pcs']].apply(pd.to_numeric)
            
            
            # 园区ID配置表创建
            df_organization = pd.DataFrame(version_organization)
            df_organization.columns = ['organization_code', 'organization_id']
            df_organization['organization_id'] = df_organization['organization_id'].apply(pd.to_numeric)
            
        print('数据表传入完成')
        return df_parent, df_storage, df_plan, df_order, df_organization
        
        
    def parent(self, df_parent):
        # 创建子母公司对照字典
        company_parent = {} # 初始化字典
        # 遍历子母公司表，将简称作为key，母公司作为值 customer_short_name:customer_parent 永能德:深圳市龙岗区南湾永能德玻璃设计商行
        for i in range(len(df_parent)):
            company_parent[df_parent['customer_short_name'][i]] = df_parent['customer_parent'][i]
        print('子母对照表创建完成')
        return company_parent
        
        
    def fuzzyMatch(self, attribute15, company_parent):
        # 定义模糊查询逻辑
        for i in company_parent: # 遍历子母公司对照字典
            # 如果备注栏里的内容包含简称或母公司，或简称或母公司包含备注栏里的内容，则匹配成功
            #if attribute15 in i or attribute15 in company_parent[i] or i in attribute15 or company_parent[i] in attribute15: 
            if attribute15!='' and (attribute15 in i or attribute15 in company_parent[i] or i in attribute15 or company_parent[i] in attribute15):
                return company_parent[i]
        return None
        
    
    def storagePlan(self, df_storage, df_plan, company_parent):
        # 交货计划匹配母公司
        df_plan['parent'] = df_plan.apply(lambda x: self.fuzzyMatch(x['customer_name'], company_parent), axis = 1) # 使用模糊匹配函数匹配出母公司信息,问题清单中涉及，可能需要修改
        #organization_id不需要删除。modify by 冯慧宇20230403
        df_plan.drop(columns='organization_id', inplace=True) # 删除无效字段
        # 添加索引列，之后库存预分配后根据索引创建唯一分配ID
        df_plan['index'] = df_plan.index
        # 库存数据匹配母公司, 根据物料编码母公司汇总库存
        df_storage['attribute15'] = df_storage['attribute15'].fillna('Null') # 用Null填补库存表中的备注空值，防止被匹配到
        df_storage['attribute15'] = df_storage.apply(lambda x: x['attribute15'].strip().split(' ')[0], axis = 1) #删除字符串前面的空格，取库存备注的首个信息
        df_storage['parent'] = df_storage.apply(lambda x: self.fuzzyMatch(x['attribute15'], company_parent), axis = 1) # 使用模糊匹配函数匹配出母公司信息
        # 根据物料编码，母公司，生产组织ID汇总库存
        df_storage = df_storage.groupby(['item_number', 'organization_id', 'parent']).agg({'available_inventory':'sum', 'available_inventory_ton':'sum'}) 
        df_storage = df_storage.reset_index(level=['item_number', 'organization_id', 'parent'])
        
        
        # 交货计划关联库存数据，问题清单中涉及，可能需要修改。解决问题2
        df = pd.merge(df_plan, df_storage, on=['item_number'], how='left')
        #df = pd.merge(df_plan, df_storage, on=['organization_id','item_number'], how='left')
        # 针对每个交货计划，根据交货量，库存量降序排列，库存available_inventory可能需要删除
        df = df.sort_values(by=['height', 'width', 'thickness', 'package_pcs', 'package_type', 'product_type', 'delivery_plan_3_day_total', 'available_inventory'], ascending=False)
        # 将交货计划为空的，或未匹配到库存的字段使用0填补
        df[['organization_id', 'delivery_plan_day_1', 'delivery_plan_day_2', 'delivery_plan_day_3', 'delivery_plan_day_4', 'delivery_plan_day_5', 'delivery_plan_3_day_total', 'delivery_plan_total', 'available_inventory', 'available_inventory_ton']] = df[['organization_id', 'delivery_plan_day_1', 'delivery_plan_day_2', 'delivery_plan_day_3', 'delivery_plan_day_4', 'delivery_plan_day_5', 'delivery_plan_3_day_total', 'delivery_plan_total', 'available_inventory', 'available_inventory_ton']].fillna(0)
        # 加排产单号
        df['plan_number'] = self.plan_number
        # 将没有关联到库存的交货计划的母公司记为空
        df['parent_y'] = df['parent_y'].fillna('')
        
        print('关联库存完成')
        return df
    
    def storageAllocation(self, df, df_organization):
        # 初始化分配库存列，计划排产列
        df = df[df['delivery_plan_3_day_total'] > 0] # 筛选未来n天小计大于0的交货计划，进行分配
        df['delivery_plan_3_day_total_dup'] = df['delivery_plan_3_day_total'] # 创建一个新列，用于保存未来n天交货量小计
        df['available_inventory_dup'] = df['available_inventory'] # 创建一个新列，用于保存库存量
        df['allocated_storage'] = 0 # 分配库存量
        df['planned_production'] = 0 # 计划生产量
        df['allocation_id'] = 0 # 排产id
        df['total_inventory'] = 0 # 总库存量
        # 园区的ID唯一标识，用于生成唯一库存分配ID
        organization_code = np.array(df_organization[df_organization['organization_code'] == self.plan_number[0:2]]['organization_id'])[0]
        
        # 库存预分配逻辑，遍历每行
        for index in df.index:
            # 根据每行的索引，找到对应行的各列数据
            index_num = np.array(df[df.index == index]['index'])[0]
            item_number = np.array(df[df.index == index]['item_number'])[0]
            parent = np.array(df[df.index == index]['parent_x'])[0]
            parent_storage = np.array(df[df.index == index]['parent_y'])[0]
            plan_number = np.array(df[df.index == index]['plan_number'])[0]
            organization_id = np.array(df[df.index == index]['organization_id'])[0]
            delivery_plan_3_day_total = np.array(df[df.index == index]['delivery_plan_3_day_total'])[0]
            available_inventory = np.array(df[df.index == index]['available_inventory'])[0]
            min_value = min(delivery_plan_3_day_total, available_inventory)
        
            # 根据排产单号+索引生成唯一分配id
            df['allocation_id'].loc[df.index==index] = int(plan_number[2::] + str(int(organization_code)) + str(index_num))
            
            # 其他母公司的不允许分配
            if parent != parent_storage:
                continue
        
            # 将表内所有同一交货计划的交货量更新为：交货量 - min(交货量，库存)，根据客户名称和玻璃规格确定交货计划
            df['delivery_plan_3_day_total'].loc[df['index']==index_num] = delivery_plan_3_day_total - min_value
            # 根据物料编码，更新这个母公司物料编码的库存为：库存量 - min(交货量，库存)
            df['available_inventory'].loc[(df['item_number'] == item_number) &
                                      (df['parent_y'] == parent) &
                                      (df['organization_id'] == organization_id)
                                     ] = available_inventory - min_value
            # 分配库存为 min(交货量，库存)
            df['allocated_storage'].loc[df.index==index] = min_value
        
        # 将表内所有同一交货计划的排产量更新为现在的交货计划3天小计
        df['planned_production'] = df['delivery_plan_3_day_total']
        # 将预先复制的交货计划小计和库存总量重新赋值
        df['delivery_plan_3_day_total'] = df['delivery_plan_3_day_total_dup']
        df['total_inventory'] = df['available_inventory_dup']
        # 删除无用字段
        df.drop(columns=['index','delivery_plan_3_day_total_dup', 'available_inventory_dup'], inplace=True)
        
        print('库存预分配完成')
        return df
           
    def output(self, df):
        # 库存预分配明细表
        df_allocated_detail = df[['allocation_id',  'plan_number', 'parent_y', 'inventory_item_id', 'organization_id', 'item_number', 'total_inventory', 'allocated_storage', 'available_inventory']]
        df_allocated_detail.rename(columns={'parent_y':'customer_parent_sn', 'total_inventory':'available_inventory', 'allocated_storage':'allocated_inventory_quantity', 'available_inventory':'remain_available_inventory_quantity'}, inplace=True)
        #不太清楚为啥要过滤
        df_allocated_detail = df_allocated_detail[df_allocated_detail['customer_parent_sn'] != '']
        
        # 库存分配汇总表，根据分配id汇总分配库存量
        df_allocated_sum = df.groupby(['allocation_id']).agg({'cust_delivery_id':'first', 'plan_number':'first', 'customer_id':'first','customer_name':'first','parent_x':'first', 'delivery_location':'first', 'thickness':'first', 'width':'first', 'height':'first', 'product_type':'first', 'package_pcs':'first', 'package_type':'first', 'drawing_number':'first', 'delivery_plan_day_1':'first', 'delivery_plan_day_2':'first', 'delivery_plan_day_3':'first', 'delivery_plan_day_4':'first', 'delivery_plan_day_5':'first', 'delivery_plan_3_day_total':'first', 'delivery_plan_total':'first', 'allocated_storage':'sum', 'planned_production':'first'})
        df_allocated_sum = df_allocated_sum.reset_index(level=['allocation_id'])
        # 字段重命名
        df_allocated_sum.rename(columns={'customer_name':'customer_sn', 'parent_x':'customer_parent_sn', 'allocated_storage':'allocated_inventory_quantity_total', 'planned_production':'plan_produce_quantity_total'}, inplace=True)
        df_allocated_sum = df_allocated_sum.sort_values(by=['plan_produce_quantity_total'],ascending=False)
        # 删除无用字段
        df.drop(columns=['parent_y', 'delivery_plan_day_4', 'delivery_plan_day_5'], inplace=True)
        
        print('输出表创建完成')
        return df_allocated_detail, df_allocated_sum
    
    
    def production(self, df):
        # 初始化生产表
        df_production = df.copy()
        # 改变母公司字段的名称
        df_production.rename(columns={'parent_x':'parent'}, inplace=True)
        # 根据分配id去重
        df_production = df_production.drop_duplicates(subset='allocation_id', keep='first')
        # 筛选预计排产大于0的交货计划
        df_production = df_production[df_production['planned_production'] > 0]
        # 初始化一些变量
        df_production['order_number'] = ''
        df_production['order_id'] = 0
        df_production['order_demand_id'] = 0
        df_production['order_demand_number'] = ''
        df_production['order_requirement_quantity'] = 0
        df_production['order_incomplete_quantity'] = 0
        df_production['remarks'] = ''
        # 将箱单位转化为吨单位
        df_production['planned_production_ton'] = round(df_production['planned_production'] * df_production['transfer_ratio'], 0)
        print('生产表初始化完成')
        return df_production
        
        
    def orderMatch(self, company_parent, df_order, df_production):
        # 匹配出母公司信息
        df_order['parent'] = df_order.apply(lambda x: self.fuzzyMatch(x['customer_name'], company_parent), axis = 1)
        # 给每条交货计划，按订单日期升序排列，取最早的订单挂排产量  ------[用order_date排序]
        # df_order['order_numbering'] = df_order.apply(lambda x: int(x['order_number'].split('-')[1]), axis = 1)
        # 将没有图纸编号的订单用空值代替，用于后面的匹配
        df_order['drawing_number'] = df_order['drawing_number'].fillna('')
        # 添加一个实时更新的订单未完成数字段
        df_order['order_incomplete_quantity_dup'] = df_order['order_incomplete_quantity']
        
        # 关联订单
        for index in df_production.index:
            customer_name = np.array(df_production[df_production.index == index]['customer_name'])[0]
            parent = np.array(df_production[df_production.index == index]['parent'])[0]
            item_number = np.array(df_production[df_production.index == index]['item_number'])[0]
            height = np.array(df_production[df_production.index == index]['height'])[0]
            width = np.array(df_production[df_production.index == index]['width'])[0]
            thickness = np.array(df_production[df_production.index == index]['thickness'])[0]
            product_type = np.array(df_production[df_production.index == index]['product_type'])[0]
            package_type = np.array(df_production[df_production.index == index]['package_type'])[0]
            package_pcs = np.array(df_production[df_production.index == index]['package_pcs'])[0]
            drawing_number = np.array(df_production[df_production.index == index]['drawing_number'])[0]
            planned_production = np.array(df_production[df_production.index == index]['planned_production'])[0]
            # 根据客户名称+物料编码匹配
            matched = df_order[(df_order['customer_name'] == customer_name) & (df_order['item_number'] == item_number) & (df_order['order_incomplete_quantity_dup'] > 0)]
            if len(matched) >= 1:
                # matched = matched.sort_values(by=['order_numbering'], ascending=True)
                matched = matched.sort_values(by=['order_date'], ascending=True)
                df_production['order_number'].loc[df_production.index==index] = matched['order_number'].iloc[0]
                df_production['organization_id'].loc[df_production.index==index] = matched['organization_id'].iloc[0]
                df_production['order_id'].loc[df_production.index==index] = matched['order_id'].iloc[0]
                df_production['order_demand_id'].loc[df_production.index==index] = matched['order_demand_id'].iloc[0]
                df_production['order_demand_number'].loc[df_production.index==index] = matched['order_demand_number'].iloc[0]
                df_production['inventory_item_id'].loc[df_production.index==index] = matched['inventory_item_id'].iloc[0]
                df_production['order_requirement_quantity'].loc[df_production.index==index] = matched['order_requirement_quantity'].iloc[0]
                df_production['order_incomplete_quantity'].loc[df_production.index==index] = matched['order_incomplete_quantity'].iloc[0]
                df_production['remarks'].loc[df_production.index==index] = matched['remarks'].iloc[0]
                # 更新订单表内的未完成数字段
                df_order['order_incomplete_quantity_dup'].loc[df_order['order_demand_id'] == matched['order_demand_id'].iloc[0]] -= planned_production
            else:
                # 根据母公司+物料编码匹配
                matched = df_order[(df_order['parent'] == parent) & (df_order['item_number'] == item_number) & (df_order['order_incomplete_quantity_dup'] > 0)]
                if len(matched) >= 1:
                    # matched = matched.sort_values(by=['order_numbering'], ascending=True)
                    matched = matched.sort_values(by=['order_date'], ascending=True)
                    df_production['customer_name'].loc[df_production.index==index] = matched['customer_name'].iloc[0]
                    df_production['parent'].loc[df_production.index==index] = matched['parent'].iloc[0]
                    df_production['order_number'].loc[df_production.index==index] = matched['order_number'].iloc[0]
                    df_production['organization_id'].loc[df_production.index==index] = matched['organization_id'].iloc[0]
                    df_production['order_id'].loc[df_production.index==index] = matched['order_id'].iloc[0]
                    df_production['order_demand_id'].loc[df_production.index==index] = matched['order_demand_id'].iloc[0]
                    df_production['order_demand_number'].loc[df_production.index==index] = matched['order_demand_number'].iloc[0]
                    df_production['inventory_item_id'].loc[df_production.index==index] = matched['inventory_item_id'].iloc[0]
                    df_production['order_requirement_quantity'].loc[df_production.index==index] = matched['order_requirement_quantity'].iloc[0]
                    df_production['order_incomplete_quantity'].loc[df_production.index==index] = matched['order_incomplete_quantity'].iloc[0]
                    df_production['remarks'].loc[df_production.index==index] = matched['remarks'].iloc[0]
                    # 更新订单表内的未完成数字段
                    df_order['order_incomplete_quantity_dup'].loc[df_order['order_demand_id'] == matched['order_demand_id'].iloc[0]] -= planned_production
                # else:
                #     # 根据玻璃规格、厚度、花型匹配
                #     matched = df_order[(df_order['height'] == height) & 
                #                       (df_order['width'] == width) & 
                #                       (df_order['thickness'] == thickness) &
                #                       (df_order['product_type'] == product_type) &
                #                       (df_order['package_type'] == package_type) &
                #                       (df_order['package_pcs'] == package_pcs) &
                #                       (df_order['drawing_number'] == drawing_number) & 
                #                       (df_order['order_incomplete_quantity_dup'] > 0)
                #                       ]
                #     if len(matched) >= 1:
                #         matched = matched.sort_values(by=['order_numbering'], ascending=True)
                #         df_production['customer_name'].loc[df_production.index==index] = matched['customer_name'].iloc[0]
                #         df_production['parent'].loc[df_production.index==index] = matched['parent'].iloc[0]
                #         df_production['order_number'].loc[df_production.index==index] = matched['order_number'].iloc[0]
                #         df_production['organization_id'].loc[df_production.index==index] = matched['organization_id'].iloc[0]
                #         df_production['order_id'].loc[df_production.index==index] = matched['order_id'].iloc[0]
                #         df_production['order_demand_id'].loc[df_production.index==index] = matched['order_demand_id'].iloc[0]
                #         df_production['order_demand_number'].loc[df_production.index==index] = matched['order_demand_number'].iloc[0]
                #         df_production['inventory_item_id'].loc[df_production.index==index] = matched['inventory_item_id'].iloc[0]
                #         df_production['order_requirement_quantity'].loc[df_production.index==index] = matched['order_requirement_quantity'].iloc[0]
                #         df_production['order_incomplete_quantity'].loc[df_production.index==index] = matched['order_incomplete_quantity'].iloc[0]
                #         df_production['remarks'].loc[df_production.index==index] = matched['remarks'].iloc[0]
                #         # 更新订单表内的未完成数字段
                #         df_order['order_incomplete_quantity_dup'].loc[df_order['order_demand_id'] == matched['order_demand_id'].iloc[0]] -= planned_production
        
        
        # 填补没有关联上订单的需求，不能为空
        df_production['order_number'] = df_production.apply(lambda x: '-' if (x['order_number'] == '')or (pd.isnull(x['order_number'])==True) else x['order_number'], axis = 1)
        df_production['order_demand_number'] = df_production.apply(lambda x: '-' if x['order_demand_number'] == '' else x['order_demand_number'], axis = 1)
        df_production['days'] = self.days
        
        # 订单关联表
        df_order_plan = df_production.copy()
        df_order_plan.rename(columns={'customer_name':'customer_sn', 'planned_production':'plan_produce_quantity', 'order_demand_id':'demand_id', 'order_demand_number':'demand_number'}, inplace=True)
        df_order_plan = df_order_plan[['plan_number', 'allocation_id', 'customer_id', 'customer_sn', 'inventory_item_id', 'item_number', 'order_number', 'order_id', 'demand_number', 'demand_id', 'order_requirement_quantity', 'order_incomplete_quantity', 'plan_produce_quantity']]
        # 初始化优先排产字段
        df_order_plan['is_prior'] = 0
    
        print('订单关联完成')
        return df_production, df_order_plan
    
    
    # 数据回写入gf_dm_model的schema
    def dmRewrite(self, df_output, table_name, host, database, user, password):
        '''结果表回写到dws数据库中'''
        # 连接数据库
        password = parse.quote_plus(password)
        SQLALCHEMY_DATABASE_URI = 'postgresql://' + user + ':'  + password + '@' + host + ':8000' + '/' + database
        engine = create_engine(SQLALCHEMY_DATABASE_URI, client_encoding='utf8')
        # 写入数据库
        df_output.to_sql(schema='gf_dm_model', con=engine, name = table_name, if_exists='append', index=False)
        
        
    # 数据回写入gf_dm_model的schema
    def dwrRewrite(self, df_output, table_name, host, database, user, password):
        '''结果表回写到dws数据库中'''
        # 连接数据库
        password = parse.quote_plus(password)
        SQLALCHEMY_DATABASE_URI = 'postgresql://' + user + ':'  + password + '@' + host + ':8000' + '/' + database
        engine = create_engine(SQLALCHEMY_DATABASE_URI, client_encoding='utf8')
        # 写入数据库
        df_output.to_sql(schema='gf_dwr_model', con=engine, name = table_name, if_exists='append', index=False)
        
    def apply(self):
        print('传入的排产单号为: ', self.plan_number)
        print('传入的天数为: ', self.days)
        ###获取配置文件里的敏感信息，进行解密
        host = self.AES_de(self.host, self.key, self.iv)
        database = self.AES_de(self.database, self.key, self.iv)
        user = self.AES_de(self.user, self.key, self.iv)
        password = self.AES_de(self.password, self.key, self.iv)
        # 读取中间表
        df_parent, df_storage, df_plan, df_order, df_organization = self.dwsConnect(host, database, user, password)
        # 创建子母公司对照字典
        company_parent = self.parent(df_parent)
        # 交货计划与库存关联
        df = self.storagePlan(df_storage, df_plan, company_parent)
        # 库存预分配主逻辑
        df = self.storageAllocation(df, df_organization)
        print(len(df))
        # # 库存明细表、汇总表、查库存表输出
        df_allocated_detail, df_allocated_sum = self.output(df)
        # # 订单匹配表
        df_production = self.production(df)
        # # # 匹配订单
        df_production, df_order_plan = self.orderMatch(company_parent, df_order, df_production)
        # # 回写结果表入数据库
        self.dmRewrite(df_allocated_detail, 'T1', host, database, user, password) # 明细表
        self.dmRewrite(df_allocated_sum, 'T2', host, database, user, password) # 汇总表
        self.dmRewrite(df_order_plan, 'T3', host, database, user, password) # 匹配订单表
        # # 回写中间表入数据库
        self.dwrRewrite(df_production, 'T6', host, database, user, password) # 中间表
        print('回写数据库完成')

#实例化与脚本执行
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("/home/qc/iiot_config/conf.ini")
    Iapa = InventoryAllocation('${PLAN_NUMBER}', '${DAYS}', config)
    # Iapa = InventoryAllocation('WH20230524001', '3', config)
    Iapa.apply()
    print('Success')