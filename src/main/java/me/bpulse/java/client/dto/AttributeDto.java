/**
 * @Copyright &copy; BPulse - http://www.bpulse.io
 * @CreationDate 23 de nov. de 2016 - BPulse-Team
 */
package me.bpulse.java.client.dto;

import java.util.List;

/**
 * @author BPulse Team <br/> 
 * 		   Copyright &copy; BPulse - http://www.bpulse.io
 * @class AttributeDto
 * @date 23 de nov. de 2016
 */
public class AttributeDto {

	private String typeId;
	private List<String> ListAttr;
	
	/**
	 * Super Constructor
	 */
	public AttributeDto() {
		super();
	}
	/**
	 * @param typeId
	 * @param listAttr
	 */
	public AttributeDto(String typeId, List<String> listAttr) {
		super();
		this.typeId = typeId;
		ListAttr = listAttr;
	}
	/**
	 * @return the typeId
	 */
	public String getTypeId() {
		return typeId;
	}
	/**
	 * @param typeId the typeId to set
	 */
	public void setTypeId(String typeId) {
		this.typeId = typeId;
	}
	/**
	 * @return the listAttr
	 */
	public List<String> getListAttr() {
		return ListAttr;
	}
	/**
	 * @param listAttr the listAttr to set
	 */
	public void setListAttr(List<String> listAttr) {
		ListAttr = listAttr;
	}
	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((typeId == null) ? 0 : typeId.hashCode());
		return result;
	}
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AttributeDto other = (AttributeDto) obj;
		if (typeId == null) {
			if (other.getTypeId() != null)
				return false;
		} else if (!typeId.equals(other.getTypeId()))
			return false;
		return true;
	}
}
