import pyjapc
from bbq import subscribe_bbq_kicked_full, subscribe_bbq_kicked
from bct import subscribe_bct

_SPS_USER = "SPS.USER.MD2"

japc = pyjapc.PyJapc(_SPS_USER, incaAcceleratorName=None)
subscribe_bbq_kicked(japc)
subscribe_bbq_kicked_full(japc)
subscribe_bct(japc)
japc.startSubscriptions()
