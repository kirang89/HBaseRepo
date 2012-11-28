package com.hbase.dataStructs;

public class ConnectionConf {

	private String zkQuorum;
	private String zkClientPort;
	private String zkZnodeParent;

	public void setConfiguration(Configuration config) {

		this.setZkClientPort(config.getZkClientPort());
		this.setZkQuorum(config.getZkQuorum());
		this.setZkZnodeParent(config.getZnodeParent());
	}

	/**
	 * @return the zkQuorum
	 */
	public String getZkQuorum() {
		return zkQuorum;
	}

	/**
	 * @param zkQuorum
	 *            the zkQuorum to set
	 */
	private void setZkQuorum(String zkQuorum) {
		this.zkQuorum = zkQuorum;
	}

	/**
	 * @return the zkClientPort
	 */
	public String getZkClientPort() {
		return zkClientPort;
	}

	/**
	 * @param zkClientPort
	 *            the zkClientPort to set
	 */
	private void setZkClientPort(String zkClientPort) {
		this.zkClientPort = zkClientPort;
	}

	/**
	 * @return the zkZnodeParent
	 */
	public String getZkZnodeParent() {
		return zkZnodeParent;
	}

	/**
	 * @param zkZnodeParent
	 *            the zkZnodeParent to set
	 */
	private void setZkZnodeParent(String zkZnodeParent) {
		this.zkZnodeParent = zkZnodeParent;
	}
}
