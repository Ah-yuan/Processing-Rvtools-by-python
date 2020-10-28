import pandas as pd
#“2020.10.18 RVTools数据处理 v5.5 v6.0”
#读写路径
fill_path = './data_input/RVTools_export_all_84.7.34.17(60).xlsx'
out_path = './data_output/84.7.34.17(60)/'

#集群单主机条件
value_df = pd.read_excel(fill_path,
                         sheet_name='vCluster', usecols='A,D', index_col=0)
value_df = value_df.loc[value_df['NumHosts'] == 1]

#集群HA/DRS开启状态

df = pd.read_excel(fill_path, sheet_name='vCluster')
HA_df = df[(df['HA enabled'] == False) | (df['DRS enabled'] == False)]
for i, v in value_df.iterrows():
    HA_df = HA_df[HA_df['Name'] != i]
HA_df.to_excel(out_path + '集群HADRS开启状态-详细信息.xlsx', index=None)

# 集群主机对存储LUN识别检查

df = pd.read_excel(fill_path, sheet_name='vMultiPath',
                    dtype={'Oper. State': 'string',
                            'Path 1 state': 'string',
                            'Path 2 state': 'string',
                            'Path 3 state': 'string',
                            'Path 4 state': 'string',
                            'Path 5 state': 'string',
                            'Path 6 state': 'string',
                            'Path 7 state': 'string',
                            'Path 8 state': 'string'
                             }
                   )
path_df =df.loc[(df['Oper. State'] != 'ok') | (df['Path 1 state'] != 'active')
                | (df['Path 2 state'] != 'active') | (df['Path 3 state'] != 'active')
                | (df['Path 4 state'] != 'active') | (df['Path 5 state'] != 'active')
                | (df['Path 6 state'] != 'active') | (df['Path 7 state'] != 'active')
                | (df['Path 8 state'] != 'active')
                ]
path_df.to_excel(out_path + '集群主机对存储LUN识别检查-详细信息.xlsx', index=None)



#集群主机网络配置检查

df = pd.read_excel(fill_path, sheet_name='vPort', usecols='A:F')
df.drop_duplicates(subset=['Cluster', 'Port Group', 'Switch', 'VLAN'], keep=False, inplace=True)
for i, v in value_df.iterrows():
    df = df[df['Cluster'] != i]
df.to_excel(out_path + '集群主机网络配置检查-详细信息.xlsx', index=None)

#集群主机内存及CPU的大小、型号检查
CPU_df = pd.read_excel(fill_path, sheet_name='vHost', usecols='A:C,E,M')
CPU_df.drop_duplicates(subset=['Cluster', 'CPU Model', '# Memory'], keep=False, inplace=True)


for i, v in value_df.iterrows():
    #print(i)
    CPU_df = CPU_df[CPU_df['Cluster'] != i]

CPU_df.to_excel(out_path + '集群主机内存及CPU检查-详细信息.xlsx', index=None)

#主机CPU负载健康状态
CPU_usage = pd.read_excel(fill_path, sheet_name='vHost', usecols='A:C,L')
CPU_usage = CPU_usage[CPU_usage['CPU usage %'] > 80]
CPU_usage.to_excel(out_path + '主机CPU负载健康状态-详细信息.xlsx', index=None)


#主机内存负载健康检查
Memory_usage = pd.read_excel(fill_path, sheet_name='vHost', usecols='A:C,N')
Memory_usage = Memory_usage[Memory_usage['Memory usage %'] >= 80]
Memory_usage.to_excel(out_path + '主机内存负载健康检查-详细信息.xlsx', index=None)


#主机网络健康状态
NIC_df = pd.read_excel(fill_path, sheet_name='vNIC', usecols='A:C,G,I')
NIC_df = NIC_df[NIC_df['Duplex'] == True]
NIC_df.drop_duplicates(subset=['Host', 'Switch'], keep=False, inplace=True)
NIC_df.to_excel(out_path + '主机网络健康状态-详细信息.xlsx', index=None)

#虚拟机健康状态
vinfo_df = pd.read_excel(fill_path, sheet_name='vInfo', usecols='A:B,D')
vinfo_df = vinfo_df[vinfo_df['Config status'] != 'green']
# vinfo_df.to_excel(out_path + 'vm_status_result.xlsx', index=None)
tools_df = pd.read_excel(fill_path, sheet_name='vTools', usecols='B:C,F')
tools_df = tools_df[tools_df['Powerstate'] != 'poweredOff']
tools_df = tools_df[tools_df['Tools'] != 'toolsOk']
#tools_df.to_excel(out_path + 'vm_tools_result.xlsx', index=None)
vm_df = pd.merge(tools_df, vinfo_df, how='outer', left_on='VM', right_on='VM')
vm_df.to_excel(out_path + '虚拟机健康状态-详细信息.xlsx', index=None)

#虚拟机快照使用情况

vSnapshot_df = pd.read_excel(fill_path, sheet_name='vSnapshot')
vSnapshot_df.to_excel(out_path + '虚拟机快照-详细信息.xlsx', index=None)

#虚拟机存储位置检查

vDisk_df = pd.read_excel(fill_path, sheet_name='vDisk', usecols='A,B,R')
vDisk_df = vDisk_df[~vDisk_df['Path'].str.contains('DCC')]
vDisk_df.to_excel(out_path + '虚拟机存储位置检查-详细信息.xlsx', index=None)


#虚拟机外设情况
vCD_df = pd.read_excel(fill_path, sheet_name='vCD', usecols='B:H')
# vCD_df = vCD_df.drop('Select', axis=1)
vCD_df = vCD_df[(vCD_df['Connected'] == True) | (vCD_df['Starts Connected'] == True)]
vCD_df.to_excel(out_path + '虚拟机外设情况-详细信息.xlsx', index=None)

#存储健康状态
vDatastore_df = pd.read_excel(fill_path, sheet_name='vDatastore',
                              usecols='A,K')
vDatastore_df = vDatastore_df[vDatastore_df['Free %'] < 20]
vDatastore_df.to_excel(out_path + '存储健康状态-详细信息.xlsx', index=None)
print("执行完毕")