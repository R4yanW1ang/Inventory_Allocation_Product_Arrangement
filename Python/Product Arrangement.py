## PYTHON 
## ******************************************************************** ##
## author: hw_wangzirui
## purpose: 排产 - 智能排产
## create time: 2023/03/13 20:55:30 GMT+08:00
## 	--来源表
## 	gf_dwi.xyg_gfoen_dwi_cust_parties
## 	gf_dwr_model.xyg_gfoen_dwr_iapa_model_alloc_tmp
## 	gf_dwr_model.xyg_gfoen_dwr_iapa_product_line_status
## 	gf_dwr_model.xyg_gfoen_dwr_iapa_product_lines
## 	gf_dm_model.xyg_gfoen_dm_iapa_model_output_produce_plan
## 	gf_dwi.xyg_gfoen_dwi_gfcrm_plan_iapa_detail
## 	--目标表
## 	xyg_gfoen_dm_iapa_model_output_produce_plan
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
    '''智能排产模型'''
    def __init__(self, plan_number, config):
        # 子母公司对照表
        # self.dim_sql_parent = "select * from gf_dwr_model.xyg_gfoen_dwr_iapa_company_shortname_parent"
        self.dim_sql_parent = "select party_name, party_short_name, parent from T1 where parent is not null and party_short_name is not null"
        
        # 库存预分配中间表
        self.dim_sql_production = f"select * from T2 where plan_number = '{plan_number}'"
        
        # 产线状态
        self.dim_sql_status = "select * from T3"
        
        # 产线能力
        self.dim_sql_capability = "select * from T4"
        
        # 前日排产
        # self.dim_sql_before = "with plan_number as (select plan_number from T5 where substring(plan_number, 3, 8) <> to_char(date_trunc('day', current_timestamp + interval '1 day'), 'yyyymmdd') and plan_status = 3 order by creation_date desc limit 1) select * from gf_dm_model.xyg_gfoen_dm_iapa_model_output_produce_plan where plan_number = (select plan_number from plan_number) and product_line <> ''" # 前日排产结果
        #self.dim_sql_before = "select * from gT6 where plan_number = 'WH20230317001'" # del
        self.dim_sql_before = "select id,cust_delivery_id,plan_number,order_number,order_id,organization_id,demand_number,demand_id,customer_sn,customer_id,inventory_item_id,item_number,thickness,width,height,product_type,package_pcs,package_type,drawing_number,order_requirement_quantity,order_incomplete_quantity,plan_produce_quantity,plan_produce_quantity_ton,transfer_ratio,workshop,product_line,prior,is_prior,remarks,dws_create_date from T where plan_number = 'WH20230605001'" # del
        # 匹配订单明细表,生产组织可能需要添加
        self.dim_sql_order = f"select allocation_id, plan_produce_quantity, is_prior from T5 where plan_number = '{plan_number}'"
        
        # 排产单号
        # self.plan_number = 'WH20230315004'
        self.plan_number = plan_number
        
        # 加解密
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
            cur.execute(self.dim_sql_production) #cursor传递sql语句
            version_production = cur.fetchall() #cursor执行sql语句并返回查询结果
            
            cur = con.cursor()
            cur.execute(self.dim_sql_status) #cursor传递sql语句
            version_status = cur.fetchall() #cursor执行sql语句并返回查询结果
            
            cur = con.cursor()
            cur.execute(self.dim_sql_capability) #cursor传递sql语句
            version_capability = cur.fetchall() #cursor执行sql语句并返回查询结果
            
            cur = con.cursor()
            cur.execute(self.dim_sql_before) #cursor传递sql语句
            version_before = cur.fetchall() #cursor执行sql语句并返回查询结果
            
            cur = con.cursor()
            cur.execute(self.dim_sql_order) #cursor传递sql语句
            version_order = cur.fetchall() #cursor执行sql语句并返回查询结果
            
        except psycopg2.DatabaseError as e: #打印错误
            print('Error:' + e)
            sys.exit(1)
        
        finally: #关闭连接
            if con:
                con.close()
            
        # 子母公司对照表
        df_parent = pd.DataFrame(version_parent)
        df_parent.columns = ['customer_name', 'customer_short_name', 'customer_parent']
        # 中间表
        df_production = pd.DataFrame(version_production)
        df_production.columns = ['cust_delivery_id', 'customer_id', 'customer_name', 'organization_id', 'delivery_location', 'inventory_item_id', 'item_number', 'thickness', 'glass_type', 'height', 'width', 'product_type', 'package_type', 'package_pcs', 'drawing_number', 'delivery_plan_day_1', 'delivery_plan_day_2', 'delivery_plan_day_3', 'delivery_plan_3_day_total', 'delivery_plan_total', 'transfer_ratio', 'dws_create_date', 'dws_update_date', 'parent', 'available_inventory', 'available_inventory_ton', 'plan_number', 'allocated_storage', 'planned_production', 'allocation_id', 'total_inventory', 'order_number', 'order_id', 'order_demand_id', 'order_demand_number', 'order_requirement_quantity', 'order_incomplete_quantity','planned_production_ton', 'remarks', 'days']
        df_production[['customer_id', 'organization_id', 'inventory_item_id', 'thickness', 'height', 'width', 'package_pcs', 'delivery_plan_day_1', 'delivery_plan_day_2', 'delivery_plan_day_3', 'delivery_plan_3_day_total', 'delivery_plan_total', 'transfer_ratio', 'available_inventory', 'available_inventory_ton', 'allocated_storage', 'planned_production', 'total_inventory',  'order_id', 'order_demand_id', 'order_requirement_quantity', 'order_incomplete_quantity', 'planned_production_ton', 'days']] = df_production[['customer_id', 'organization_id', 'inventory_item_id', 'thickness', 'height', 'width', 'package_pcs', 'delivery_plan_day_1', 'delivery_plan_day_2', 'delivery_plan_day_3', 'delivery_plan_3_day_total', 'delivery_plan_total', 'transfer_ratio', 'available_inventory', 'available_inventory_ton', 'allocated_storage', 'planned_production', 'total_inventory',  'order_id', 'order_demand_id', 'order_requirement_quantity', 'order_incomplete_quantity', 'planned_production_ton', 'days']].apply(pd.to_numeric)
        df_production = df_production[['cust_delivery_id', 'customer_id', 'customer_name', 'delivery_location', 'inventory_item_id', 'item_number', 'thickness', 'glass_type', 'height', 'width', 'product_type', 'package_type', 'package_pcs', 'drawing_number', 'transfer_ratio', 'parent', 'plan_number', 'allocation_id', 'order_number', 'order_id', 'order_demand_id', 'order_demand_number', 'order_requirement_quantity', 'order_incomplete_quantity', 'remarks', 'days', 'dws_create_date']]
        df_production['drawing_number'] = df_production['drawing_number'].fillna('') #填补图纸编号为空的
        df_production['transfer_ratio'] = df_production['transfer_ratio'].fillna(1.8) #填补转换系数为空的 
        days = df_production['days'].iloc[0] # 抓取库存预分配里的排产天数
        
        
        # 产线状态
        df_status = pd.DataFrame(version_status)
        df_status.columns = ['product_line', 'status_date', 'product_status', 'dws_create_date', 'dws_update_date']
        df_status[['product_status']] = df_status[['product_status']].apply(pd.to_numeric)
        
        
        # 产线能力
        df_capability = pd.DataFrame(version_capability)
        df_capability.columns = ['product_line', 'hearth_size', 'organization_id', 'thickness', 'thickness_code_1_6', 'thickness_code_2_0', 'thickness_code_2_8', 'thickness_code_3_2', 'second_coating_flag', 'silk_screen_flag', 'max_size_silk_screen_length', 'max_size_silk_screen_width', 'max_size_length', 'max_size_width','min_size_silk_screen_length', 'min_size_silk_screen_width', 'min_size_length', 'min_size_width', 'max_capacity_tempered', 'workshop', 'dws_create_date', 'dws_update_date']
        df_capability[['organization_id', 'hearth_size', 'thickness', 'thickness_code_1_6', 'thickness_code_2_0', 'thickness_code_2_8', 'thickness_code_3_2', 'second_coating_flag', 'silk_screen_flag', 'max_size_silk_screen_length', 'max_size_silk_screen_width', 'max_size_length', 'max_size_width','min_size_silk_screen_length', 'min_size_silk_screen_width', 'min_size_length', 'min_size_width', 'max_capacity_tempered']] = df_capability[['organization_id', 'hearth_size', 'thickness', 'thickness_code_1_6', 'thickness_code_2_0', 'thickness_code_2_8', 'thickness_code_3_2', 'second_coating_flag', 'silk_screen_flag', 'max_size_silk_screen_length', 'max_size_silk_screen_width', 'max_size_length', 'max_size_width', 'min_size_silk_screen_length', 'min_size_silk_screen_width', 'min_size_length', 'min_size_width','max_capacity_tempered']].apply(pd.to_numeric)
        df_capability['product_line'] = df_capability.apply(lambda x: int(x['product_line'].split('W')[1]), axis = 1) #将钢化产线字段里的W去掉，方便后续的处理
        
        
        # 前日排产
        df_before = pd.DataFrame(version_before)
        df_before.columns = ['id', 'cust_delivery_id', 'plan_number', 'order_number', 'order_id', 'organization_id', 'demand_number', 'demand_id', 'customer_name', 'customer_id', 'inventory_item_id', 'item_number', 'thickness', 'width', 'height', 'product_type', 'package_pcs', 'package_type', 'drawing_number', 'order_requirement_quantity', 'order_incomplete_quantity', 'arranged_production', 'arranged_production_ton', 'transfer_ratio', 'workshop', 'product_line', 'prior', 'is_prior', 'remarks', 'dws_create_date']
        df_before[['order_id', 'organization_id', 'demand_id', 'customer_id', 'inventory_item_id', 'thickness', 'width', 'height', 'package_pcs', 'order_requirement_quantity', 'order_incomplete_quantity', 'arranged_production', 'arranged_production_ton', 'transfer_ratio', 'prior', 'is_prior']] = df_before[['order_id', 'organization_id', 'demand_id', 'customer_id', 'inventory_item_id', 'thickness', 'width', 'height', 'package_pcs', 'order_requirement_quantity', 'order_incomplete_quantity', 'arranged_production', 'arranged_production_ton', 'transfer_ratio', 'prior', 'is_prior']].apply(pd.to_numeric)
        # 创建一个前日排产的复制表，如果排产后还有空产线，需要用这张表来关联这些产线前日排产的规格。
        df_before_dup = df_before.copy()
        df_before_dup.rename(columns={'arranged_production':'planned_production', 'arranged_production_ton':'planned_production_ton'}, inplace=True)
        df_before.drop(columns=['remarks', 'workshop', 'organization_id'], inplace=True)
        df_before['drawing_number'] = df_before['drawing_number'].fillna('') 

        
        # 匹配订单
        df_order = pd.DataFrame(version_order)
        df_order.columns = ['allocation_id', 'planned_production', 'is_prior']
        df_order[['planned_production', 'is_prior']] = df_order[['planned_production', 'is_prior']].apply(pd.to_numeric)
        
        df_production = df_production.merge(df_order, on=['allocation_id'], how='inner')
        df_production['planned_production_ton'] = df_production.apply(lambda x: round(x['planned_production'] * x['transfer_ratio'], 0), axis=1) #填补计划生产吨数为空的
        
        return df_parent, df_production, df_status, df_capability, df_before, df_before_dup, df_order, days
    
    
    # 创建字母公司对照字典
    def parent(self, df_parent):
        company_parent = {}
        for i in range(len(df_parent)):
            company_parent[df_parent['customer_short_name'][i]] = df_parent['customer_parent'][i]
        return company_parent
        
        
    # 定义模糊查询逻辑
    def fuzzyMatch(self, attribute15, company_parent):
        for i in company_parent:
            if attribute15 in i or attribute15 in company_parent[i] or i in attribute15 or company_parent[i] in attribute15:
                return company_parent[i]
        return None
    
    
    # 查询出今日在产产线，为产线增加最大产能字段
    def productLine(self, company_parent, df_before, df_status, df_capability, days):
        # 解析产线字段
        df_before['product_line'] = df_before.apply(lambda x: int(x['product_line'].split('线')[0]), axis=1)
        df_before['parent'] = df_before.apply(lambda x: self.fuzzyMatch(x['customer_name'], company_parent), axis = 1)
        
        # 创建空产线表
        normal_pl = list(df_status[df_status['product_status'] == 0]['product_line'])
        empty_pl = []
        for pl in normal_pl:
            empty_pl.append(int(pl.split('W')[1]))
        # 删除重复的
        empty_pl = list(set(empty_pl))
        
        
        # 筛选出昨日排产的产线中，今日还在产的产线
        df_before['product_line'] = df_before['product_line'].apply(pd.to_numeric)
        df_before = df_before[df_before['product_line'].isin(empty_pl)]
        
        
        # 通过merge添加最大产能字段
        df_before = pd.merge(df_before, df_capability[['organization_id', 'product_line', 'thickness', 'max_capacity_tempered', 'workshop']], on=['product_line', 'thickness'], how='inner')
        # 根据所选天数，将产线最大产能多倍扩大
        df_before['max_capacity_tempered'] = df_before['max_capacity_tempered'] * days
        df_before['remain_capacity'] = df_before['max_capacity_tempered']
        
        return df_before, empty_pl
    
    
    # 更新空闲产线与剩余产能函数，df_before是昨日排产结果。df_production_before是最终输出的今日排产结果
    def emptyplUpdate(self, df_before, df_production_before):
        # 删除计划生产量列
        df_before.drop(columns='arranged_production_ton', inplace=True)
        # 汇总各产线目前生产量
        df_production_before = df_production_before.groupby(['product_line']).agg({'planned_production_ton':'sum'}).reset_index(level='product_line')
        # 和产线能力表关联
        df_before = pd.merge(df_before, df_production_before[['product_line', 'planned_production_ton']], on='product_line', how='left')
        df_before.rename(columns={'planned_production_ton':'arranged_production_ton'}, inplace=True)
        # 更新剩余产能，为产线最大产能产能 - 当前产能
        df_before['arranged_production_ton'] = df_before['arranged_production_ton'].fillna(0)
        df_before['remain_capacity'] = df_before['max_capacity_tempered'] - df_before['arranged_production_ton']
        return df_before
    
    
    def merge(self, df_production, df_before, merge_columns):
        # 初始化变量
        planned_production_ton = df_production['planned_production_ton'].iloc[0]
        df_result_tmp = pd.DataFrame()
        # df_before表中需要的字段
        df_before_columns = ['product_type', 'organization_id', 'workshop', 'product_line', 'prior', 'remain_capacity', 'arranged_production_ton']
        # 通过字段merge
        merged = pd.merge(df_production, df_before[merge_columns + df_before_columns], on=merge_columns, how='inner')
        # 根据花型继续筛选，需要添加else，如果不是一次镀膜或者二次镀膜需要严格匹配df_production['product_type']=merged['product_type_y']
        if df_production['product_type'].iloc[0] in ('LAR E', 'LAR B'):
            merged = merged[(merged['product_type_y'].isin(['LAR E', 'LAR B'])) & (merged['remain_capacity'] > 0)]
        elif df_production['product_type'].iloc[0] in ('LAR HB','LAR HD','LAR HE','LAR ND','LAR NE','LAR NB'):
            merged = merged[(merged['product_type_y'].isin(['LAR HB','LAR HD','LAR HE','LAR ND','LAR NE','LAR NB'])) & (merged['remain_capacity'] > 0)]
        elif df_production['product_type'].iloc[0] in ('3back-丝印白釉','3back'):
            merged = merged[(merged['product_type_y'].isin(['3back-丝印白釉','3back'])) & (merged['remain_capacity'] > 0)]
        # 保留需求的花型
        merged.drop(columns = 'product_type_y', inplace=True)
        merged.rename(columns={'product_type_x':'product_type'}, inplace=True)
        merged = merged.drop_duplicates(subset=['product_line'], keep='first') #每个产线保留一条
        # 结果
        if len(merged) == 0:
            return df_result_tmp, planned_production_ton
        # 开始分配，arranged_production_ton已经安排的产量
        merged = merged.sort_values(by='arranged_production_ton', ascending=True)
        for index in merged.index:
            remain_capacity = np.array(merged[merged.index==index]['remain_capacity'])[0] # 获得剩余产能
            transfer_ratio = np.array(merged[merged.index==index]['transfer_ratio'])[0] # 获得单位转化系数
            if planned_production_ton > 0:
                merged['planned_production_ton'] = min(planned_production_ton, remain_capacity)
                merged['planned_production'] = round(min(planned_production_ton, remain_capacity) / transfer_ratio, 0)
                planned_production_ton -= min(planned_production_ton, remain_capacity)
                df_result_tmp = df_result_tmp.append(merged[merged.index==index])
        return df_result_tmp, planned_production_ton
    
    
    def allocateBefore(self, df_production, df_before):
        # 根据优先排产和生产量倒序排列
        df_production = df_production.sort_values(by=['is_prior', 'planned_production_ton'], ascending=False)
        # 初始化匹配上的和未匹配上的
        df_production_before = pd.DataFrame(columns=['cust_delivery_id', 'plan_number', 'order_number', 'order_id', 'organization_id', 'parent', 'order_demand_number', 'order_demand_id', 'customer_name', 'customer_id', 'inventory_item_id', 'item_number', 'thickness', 'width', 'height', 'product_type', 'package_pcs', 'package_type', 'drawing_number', 'order_requirement_quantity', 'order_incomplete_quantity', 'planned_production', 'planned_production_ton', 'transfer_ratio', 'workshop', 'product_line', 'prior', 'is_prior', 'remarks'])
        df_unmatched = pd.DataFrame(columns=df_production.columns)
        merge_columns_list = [['order_number', 'parent', 'height', 'width', 'thickness', 'package_pcs', 'package_type', 'drawing_number'],
                              ['order_number', 'parent', 'height', 'width', 'thickness', 'drawing_number'],
                              ['parent', 'height', 'width', 'thickness', 'drawing_number'],
                              ['height', 'width', 'thickness', 'drawing_number'],
                              ['width', 'thickness', 'drawing_number'],
                              ['width', 'thickness']
                            ]
        for index in df_production.index:
            for merge_columns in merge_columns_list:
                # 根据订单号+客户名称+长+宽+厚度+花型+包装方式+片数+图纸编号关联前日排产结果
                df_production_slice = df_production[df_production.index==index] # 切片
                df_result_tmp, planned_production_ton = self.merge(df_production_slice, df_before, merge_columns) # 代入函数中
                if len(df_result_tmp) > 0:
                    df_production_before = df_production_before.append(df_result_tmp[df_production_before.columns]) # 将产线结果加入到最终结果表中
                    df_before = self.emptyplUpdate(df_before, df_production_before) # 更新剩余产能
                # 如无法满足需求，更新计划产量，进入下一步
                if planned_production_ton == 0:
                    break
                else:
                    df_production['planned_production_ton'].loc[df_production.index==index] = planned_production_ton
            # 几轮下来如果还有余量没有安排生产，进入下一轮产品参数
            if planned_production_ton > 0:
                df_unmatched = df_unmatched.append(df_production[df_production.index==index])
        
                            
        # 重置索引，使索引不唯一
        df_production_before = df_production_before.reset_index()
        df_production_before.drop(columns=['index'], inplace=True)
        
        # 按计划产能倒序
        df_unmatched = df_unmatched.sort_values(by=['item_number', 'planned_production_ton'], ascending=False)
        return df_before, df_production_before, df_unmatched



    #  将生产量分配到可生产该需求的产线上
    def plCapability(self, df_unmatched_index, df_before_pl, df_production_before):
        df_before_pl = df_before_pl.sort_values(by='arranged_production_ton', ascending=True)
        transfer_ratio = df_unmatched_index['transfer_ratio'].iloc[0] # 箱转吨转换系数
        total_productivity_ton = df_unmatched_index['planned_production_ton'].iloc[0] # 计划生产量
        df_before_pl = df_before_pl.drop_duplicates(subset=['product_line'], keep='first')
        # df_unmatched_index.reset_index(inplace=True, drop=True)
        # 每行遍历
        for index in df_before_pl.index:
            if total_productivity_ton > 0:
                product_line_rc = np.array(df_before_pl[df_before_pl.index==index]['remain_capacity'])[0]
                min_value = min(product_line_rc, total_productivity_ton)
                df_unmatched_index['planned_production_ton'] = min_value
                df_unmatched_index['planned_production'] = round(min_value / transfer_ratio, 0)
                total_productivity_ton = total_productivity_ton - min_value # 实时更新剩余产能

                df_unmatched_index_dup = df_unmatched_index.copy()
                df_unmatched_index_dup[['organization_id', 'workshop', 'product_line', 'prior']] = list(df_before_pl[df_before_pl.index==index][['organization_id', 'workshop', 'product_line', 'prior']].values)

                df_production_before = df_production_before.append(df_unmatched_index_dup[df_production_before.columns])
        total_productivity = round(total_productivity_ton / transfer_ratio, 0)
        return total_productivity, total_productivity_ton, df_production_before



    def allocateSpec(self, df_unmatched, df_before, df_capability, df_production_before):
        # 初始化未安排生产的需求
        df_unmatched_again = pd.DataFrame(columns=df_unmatched.columns)
        df_unmatched = df_unmatched.sort_values(by=['is_prior', 'item_number', 'planned_production_ton'], ascending=False)
        # 匹配产线
        for index in df_unmatched.index:
            df_unmatched_index = df_unmatched[df_unmatched.index==index]
            height = np.array(df_unmatched[df_unmatched.index==index]['height'])[0]
            width = np.array(df_unmatched[df_unmatched.index==index]['width'])[0]
            thickness = np.array(df_unmatched[df_unmatched.index==index]['thickness'])[0]
            product_type = np.array(df_unmatched[df_unmatched.index==index]['product_type'])[0]
            # 二次镀膜
            if product_type in ('LAR HB','LAR HD','LAR HE','LAR ND','LAR NE', 'LAR NB'):
                matched = df_capability[(df_capability['max_size_length'] >= height) &
                                (df_capability['min_size_length'] <= height) &
                                (df_capability['max_size_width'] >= width) &
                                (df_capability['min_size_width'] <= width) &
                                (df_capability['second_coating_flag'] == 1) &
                                (df_capability['thickness_code_' + str(thickness).split('.')[0] + '_' + str(thickness).split('.')[1]] == 1)
                               ]
                # 根据产线能力筛选出可以生产此产品的产线
                matched_pl = list(matched['product_line'])
                # df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & (df_before['width'] == width)]
                #df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness)]
                #df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & (df_before['product_type'].iloc[0] in ('LAR HB','LAR HD','LAR HE','LAR ND','LAR NE','LAR NB'))]
                df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & (df_before['product_type'].isin(['LAR HB','LAR HD','LAR HE','LAR ND','LAR NE','LAR NB']))]
                if len(df_before_pl) == 0:
                    df_unmatched_again = df_unmatched_again.append(df_unmatched[df_unmatched.index==index])
                    continue
                # 分配生产量到产线上
                total_productivity, total_productivity_ton, df_production_before = self.plCapability(df_unmatched_index, df_before_pl, df_production_before)
                if total_productivity_ton > 0:
                    df_unmatched_index['planned_production'].iloc[df_unmatched_index.index==index] = total_productivity
                    df_unmatched_index['planned_production_ton'].iloc[df_unmatched_index.index==index] = total_productivity_ton
                    df_unmatched_again = df_unmatched_again.append(df_unmatched_index[df_unmatched_index.index==index])
                # 更新剩余产能
                df_before = self.emptyplUpdate(df_before, df_production_before)
            elif 'back' in product_type:
                matched = df_capability[(df_capability['max_size_length'] >= height) &
                                        (df_capability['min_size_length'] <= height) &
                                        (df_capability['max_size_width'] >= width) &
                                        (df_capability['min_size_width'] <= width) &
                                        (df_capability['silk_screen_flag'] == 1) &
                                        (df_capability['thickness_code_' + str(thickness).split('.')[0] + '_' + str(thickness).split('.')[1]] == 1)
                                       ]
                matched_pl = list(matched['product_line'])
                # df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & (df_before['width'] == width)]
                #添加花型严格匹配逻辑df_before['product_type'] in 'back'
                #df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & ('back' in df_before['product_type'].iloc[0])]
                df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & (df_before['product_type'].isin(['3back','3back-丝印白釉']))]
                if len(df_before_pl) == 0:
                    df_unmatched_again = df_unmatched_again.append(df_unmatched[df_unmatched.index==index])
                    continue
                # 分配生产量到产线上
                total_productivity, total_productivity_ton, df_production_before = self.plCapability(df_unmatched_index, df_before_pl, df_production_before)
                if total_productivity_ton > 0:
                    df_unmatched_index['planned_production'].iloc[df_unmatched_index.index==index] = total_productivity
                    df_unmatched_index['planned_production_ton'].iloc[df_unmatched_index.index==index] = total_productivity_ton
                    df_unmatched_again = df_unmatched_again.append(df_unmatched_index[df_unmatched_index.index==index])
                # 更新剩余产能
                df_before = self.emptyplUpdate(df_before, df_production_before)
            # 正常花型
            else:
                matched = df_capability[(df_capability['max_size_length'] >= height) &
                                (df_capability['min_size_length'] <= height) &
                                (df_capability['max_size_width'] >= width) &
                                (df_capability['min_size_width'] <= width) &
                                (df_capability['thickness_code_' + str(thickness).split('.')[0] + '_' + str(thickness).split('.')[1]] == 1)
                               ]
                matched_pl = list(matched['product_line'])
                # df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & (df_before['width'] == width)]
                #添加花型的过滤
                #df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & (df_before['product_type'].iloc[0] not in ('LAR HB','LAR HD','LAR HE','LAR ND','LAR NE', 'LAR NB')) & ('back' not in df_before['product_type'].iloc[0])]
                df_before_pl = df_before[(df_before['product_line'].isin(matched_pl)) & (df_before['remain_capacity'] > 0) & (df_before['thickness'] == thickness) & (df_before['product_type'].isin(['LAR E', 'LAR B']))]
                if len(df_before_pl) == 0:
                    df_unmatched_again = df_unmatched_again.append(df_unmatched[df_unmatched.index==index])
                    continue
                # 分配生产量到产线上
                total_productivity, total_productivity_ton, df_production_before = self.plCapability(df_unmatched_index, df_before_pl, df_production_before)
                if total_productivity_ton > 0:
                    df_unmatched_index['planned_production'].iloc[df_unmatched_index.index==index] = total_productivity
                    df_unmatched_index['planned_production_ton'].iloc[df_unmatched_index.index==index] = total_productivity_ton
                    df_unmatched_again = df_unmatched_again.append(df_unmatched_index[df_unmatched_index.index==index])
                # 更新剩余产能
                df_before = self.emptyplUpdate(df_before, df_production_before)
        
        return df_production_before, df_unmatched_again, df_before


    def formatChange(self, df_production_before, df_unmatched_again, df_before, df_before_dup, empty_pl):
        # 将相同的需求合并
        columns = ['customer_name', 'thickness', 'width', 'height', 'product_type', 'package_pcs', 'package_type', 'drawing_number', 'product_line']
        df_production_before = df_production_before.groupby(columns).agg({'cust_delivery_id':'first', 'plan_number':'first', 'order_number':'first', 'order_id':'first', 'organization_id':'first', 'parent':'first', 'order_demand_number':'first', 'order_demand_id':'first', 'customer_id':'first', 'item_number':'first', 'inventory_item_id':'first', 'order_requirement_quantity':'first', 'order_incomplete_quantity':'first', 'planned_production':'sum', 'planned_production_ton':'sum', 'transfer_ratio':'first', 'workshop':'first', 'prior':'first', 'is_prior':'first', 'remarks':'first'}).reset_index(level=columns)
        
        # 产线没有安排生产的，沿用昨天的生产安排
        df_before_dup['plan_number'] = self.plan_number
        df_before_dup['product_line'] = df_before_dup.apply(lambda x: int(x['product_line'].split('线')[0]), axis=1)
        df_before_dup = df_before_dup[df_before_dup['product_line'].isin(empty_pl)]
        df_before_dup.rename(columns={'demand_id':'order_demand_id', 'demand_number':'order_demand_number'}, inplace=True)
        df_before = df_before.drop_duplicates(subset=['product_line'], keep='first')
        df_before_dup = pd.merge(df_before_dup, df_before[['product_line', 'arranged_production_ton', 'parent']], on='product_line', how='inner')
        df_production_before = df_production_before.append(df_before_dup[df_before_dup['arranged_production_ton'] == 0][df_production_before.columns])


        # 字段名称修改 + 数据类型转化
        df_production_before = df_production_before.reset_index()
        df_production_before.drop(columns=['index'], inplace=True)
        df_production_before['product_line'] = df_production_before['product_line'].apply(pd.to_numeric)
        # df_production_before = df_production_before.sort_values(by=['product_line', 'planned_production_ton'], ascending=False)
        
        
        
        # 初始化产线与优先级的字典，每条产线上的优先级开始时默认为1，没检测到一条产线后优先级+1
        dict_priority = {}
        for product_line in set(df_production_before['product_line']):
            dict_priority[product_line] = 1
        for index in df_production_before.index:
            product_line = np.array(df_production_before[df_production_before.index==index]['product_line'])[0] # 获取这一行的产线
            df_production_before['prior'].iloc[df_production_before.index==index] = dict_priority[product_line] # 将字典中此条产线对应的优先级赋值
            dict_priority[product_line] += 1 # 将字典中此条产线对应的优先级加一
        # 调整产线上的优先级，最先被分配到产线上的产品为优1，以此类推
        # for index in df_production_before.index:
        #     if df_production_before[df_production_before.index==index]['product_type'].iloc[0] in ('LAR HB','LAR HD','LAR HE','LAR ND','LAR NE', 'LAR NB'):
        #         print(df_production_before[df_production_before.index==index]['product_type'].iloc[0])
        #         product_line = np.array(df_production_before[df_production_before.index==index]['product_line'])[0] # 获取这一行的产线
        #         df_production_before['prior'].iloc[df_production_before.index==index] = dict_priority[product_line] # 将字典中此条产线对应的优先级赋值
        #         dict_priority[product_line] += 1 # 将字典中此条产线对应的优先级加一
        # for index in df_production_before.index:
        #     if df_production_before[df_production_before.index==index]['product_type'].iloc[0] not in ('LAR HB','LAR HD','LAR HE','LAR ND','LAR NE', 'LAR NB'):
        #         print(df_production_before[df_production_before.index==index]['product_type'].iloc[0])
        #         product_line = np.array(df_production_before[df_production_before.index==index]['product_line'])[0] # 获取这一行的产线
        #         df_production_before['prior'].iloc[df_production_before.index==index] = dict_priority[product_line] # 将字典中此条产线对应的优先级赋值
        #         dict_priority[product_line] += 1 # 将字典中此条产线对应的优先级加一
        # 根据输出所需的格式调整产线及优先级顺序，为升序
        df_production_before = df_production_before.sort_values(by=['product_line', 'prior'])



        # 输出表规范整理
        df_production_before['product_line'] = df_production_before.apply(lambda x: str(int(x['product_line'])) + '线', axis = 1)
        
        # 将没排产的需求也加入结果表中
        df_unmatched_again['product_line'] = ''
        df_unmatched_again['prior'] = 0
        df_unmatched_again['workshop'] = ''
        df_unmatched_again['organization_id'] = 0
        df_production_before = df_production_before.append(df_unmatched_again[df_production_before.columns])
        
        # 输出最终结果
        df_production_before['remarks'] = ''
        df_production_before = df_production_before[['plan_number', 'order_number', 'order_id', 'organization_id', 'order_demand_number', 'order_demand_id', 'customer_name', 'customer_id', 'inventory_item_id', 'item_number', 'thickness', 'width', 'height', 'product_type', 'package_pcs', 'package_type', 'drawing_number', 'order_requirement_quantity', 'order_incomplete_quantity', 'planned_production', 'planned_production_ton', 'transfer_ratio', 'workshop', 'product_line', 'prior', 'is_prior', 'remarks']]
        df_production_before['planned_production'] = df_production_before.apply(lambda x: round(x['planned_production_ton'] / x['transfer_ratio'], 0), axis = 1) # new
        df_production_before['prior'] = df_production_before.apply(lambda x: int(x['prior']), axis = 1)
        df_production_before.rename(columns={'customer_name':'customer_sn', 'planned_production':'plan_produce_quantity', 'planned_production_ton':'plan_produce_quantity_ton', 'order_demand_number':'demand_number', 'order_demand_id':'demand_id'}, inplace=True)
        
        return df_production_before
        
        
    # 数据回写入gf_dm_model的schema
    def dataRewrite(self, df_output, table_name, host, database, user, password):
        '''结果表回写到dws数据库中'''
        # 连接数据库
        password = parse.quote_plus(password)
        SQLALCHEMY_DATABASE_URI = 'postgresql://' + user + ':'  + password + '@' + host + ':8000' + '/' + database
        engine = create_engine(SQLALCHEMY_DATABASE_URI, client_encoding='utf8')
        # 写入数据库
        df_output.to_sql(schema='gf_dm_model', con=engine, name = table_name, if_exists='append', index=False)
    
    
    def apply(self):
        print('传入的排产单号为:', self.plan_number)
        ###获取配置文件里的敏感信息，进行解密
        host = self.AES_de(self.host, self.key, self.iv)
        database = self.AES_de(self.database, self.key, self.iv)
        user = self.AES_de(self.user, self.key, self.iv)
        password = self.AES_de(self.password, self.key, self.iv)
        # 导入表格
        df_parent, df_production, df_status, df_capability, df_before, df_before_dup, df_order, days = self.dwsConnect(host, database, user, password)
        # 创建子母公司对照字典
        company_parent = self.parent(df_parent)
        # 筛选今日在产的产线
        df_before, empty_pl = self.productLine(company_parent, df_before, df_status, df_capability, days)
        # 关联前日排产结果
        df_before, df_production_before, df_unmatched = self.allocateBefore(df_production, df_before)
        # 根据产线能力分配剩余需求
        df_production_before, df_unmatched_again, df_before = self.allocateSpec(df_unmatched, df_before, df_capability, df_production_before)
        # 格式调整+优先级调整
        df_production_before = self.formatChange(df_production_before, df_unmatched_again, df_before, df_before_dup, empty_pl)
        # 回写数据表
        self.dataRewrite(df_production_before, 'T10', host, database, user, password)
        
#实例化与脚本执行
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("/home/qc/iiot_config/conf.ini")
    Iapa = InventoryAllocation('${PLAN_NUMBER}', config)
    # Iapa = InventoryAllocation('WH20230315004', config)
    Iapa.apply()
    print('Success')