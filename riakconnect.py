from riak import RiakClient, RiakNode
import time,calendar


#Riak Dev Nodes
def dev_nodes():

    riak_dev1 = RiakNode(host='devbpp01',http_port=8098,pb_port=8087)
    riak_dev2 = RiakNode(host='devbpp02',http_port=8098,pb_port=8087)
    riak_dev3 = RiakNode(host='devbpp03',http_port=8098,pb_port=8087)

#Create RC_DEV Riak Client and test the connectivity

    RC_DEV = RiakClient(protocol='http',nodes=[riak_dev1,riak_dev2,riak_dev3])
    alive_or_not = RC_DEV.ping()

    print(alive_or_not)
    if not alive_or_not:
        print("Cant Connect to QA Servers... Exiting!!!!")
        exit()

    return RC_DEV


#Riak QA_A nodes
def qaa_nodes():
    riak_qa1 = RiakNode(host='qaabpp01',http_port=8098,pb_port=8087)
    riak_qa2 = RiakNode(host='qaabpp02',http_port=8098,pb_port=8087)

#Create RC_QA Riak Client and test the connectivity
    RC_QA = RiakClient(protocol='http',nodes=[riak_qa1,riak_qa2])
    alive_or_not = RC_QA.ping()

    if not alive_or_not:
        print("Cant Connect to QAA Servers... Exiting!!!!")
        exit()

    return RC_QA

#Riak QA_B nodes
def qab_nodes():
    riak_qa1 = RiakNode(host='qabbpp01',http_port=8098,pb_port=8087)
    riak_qa2 = RiakNode(host='qabbpp02',http_port=8098,pb_port=8087)

#Create RC_QA Riak Client and test the connectivity

    RC_QA = RiakClient(protocol='http',nodes=[riak_qa1,riak_qa2])
    alive_or_not = RC_QA.ping()

    if not alive_or_not:
        print("Cant Connect to QAB Servers... Exiting!!!!")
        exit()

    return RC_QA


#Riak ALL QA Nodes
def qa_nodes():


    riak_qa1 = RiakNode(host='qaabpp01',http_port=8098,pb_port=8087)
    riak_qa2 = RiakNode(host='qaabpp02',http_port=8098,pb_port=8087)
    riak_qa3 = RiakNode(host='qabbpp01',http_port=8098,pb_port=8087)
    riak_qa4 = RiakNode(host='qabbpp02',http_port=8098,pb_port=8087)

#Create RC_QA Riak Client and test the connectivity

    RC_QA = RiakClient(protocol='http',nodes=[riak_qa1,riak_qa2,riak_qa3,riak_qa4])
    alive_or_not = RC_QA.ping()


    if not alive_or_not:
        print("Cant Connect to QA Servers... Exiting!!!!")
        exit()

    return RC_QA

#Riak Live Nodes
def live_nodes():

    riak_live1 = RiakNode(host='la1bpp01',http_port=8098,pb_port=8087)
    riak2_live2 = RiakNode(host='la2bpp01',http_port=8098,pb_port=8087)
    riak3_live3 = RiakNode(host='la3bpp01',http_port=8098,pb_port=8087)
    riak4_live4 = RiakNode(host='la4bpp01',http_port=8098,pb_port=8087)
    riak5_live5 = RiakNode(host='lb1bpp01',http_port=8098,pb_port=8087)
    riak6_live6= RiakNode(host='lb2bpp01',http_port=8098,pb_port=8087)
    riak7_live7 = RiakNode(host='lb3bpp01',http_port=8098,pb_port=8087)
    riak8_live8 = RiakNode(host='lb4bpp01',http_port=8098,pb_port=8087)

#Create RC_LIVE Riak Client and test the connectivity

    RC_LIVE = RiakClient(protocol='http',nodes=[riak_live1,riak2_live2,riak3_live3,riak4_live4,riak5_live5,riak6_live6,riak7_live7,riak8_live8])
    alive_or_not = RC_LIVE.ping()


    if not alive_or_not != 'True':
        print("Cant Connect to LIVE  Servers... Exiting!!!!")
        exit()
    return RC_LIVE

def liveb_nodes():

    riak_live1 = RiakNode(host='la1bpp01',http_port=8098,pb_port=8087)
    riak2_live2 = RiakNode(host='la2bpp01',http_port=8098,pb_port=8087)
    riak3_live3 = RiakNode(host='la3bpp01',http_port=8098,pb_port=8087)
    riak4_live4 = RiakNode(host='la4bpp01',http_port=8098,pb_port=8087)

#Create RC_LIVE Riak Client and test the connectivity

    RC_LIVE = RiakClient(protocol='http',nodes=[riak_live1,riak2_live2,riak3_live3,riak4_live4])
    alive_or_not = RC_LIVE.ping()


    if not alive_or_not != 'True':
        print("Cant Connect to LIVE A Servers... Exiting!!!!")
        exit()
    return RC_LIVE


def liveb_nodes():

    riak_live1 = RiakNode(host='lb1bpp01',http_port=8098,pb_port=8087)
    riak2_live2 = RiakNode(host='lb2bpp01',http_port=8098,pb_port=8087)
    riak3_live3 = RiakNode(host='lb3bpp01',http_port=8098,pb_port=8087)
    riak4_live4 = RiakNode(host='lb4bpp01',http_port=8098,pb_port=8087)

#Create RC_LIVE Riak Client and test the connectivity

    RC_LIVE = RiakClient(protocol='http',nodes=[riak_live1,riak2_live2,riak3_live3,riak4_live4])
    alive_or_not = RC_LIVE.ping()


    if not alive_or_not != 'True':
        print("Cant Connect to LIVE B Servers... Exiting!!!!")
        exit()
    return RC_LIVE

