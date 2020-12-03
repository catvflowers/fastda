package com.sm.fastda.basic;

import com.sm.fastda.annotation.*;

public class DataLink {
	@Id
	@Column
	public Object pk;
	@Column
	public Object fk;
	
	@Override
	public boolean equals(Object obj)  
    {  
        if (this == obj) return true;  
        if (obj == null) return false;  
        if (getClass() != obj.getClass()) return false;  
        final DataLink other = (DataLink) obj; 
        boolean flag1=pk==null||fk==null;
        if(flag1) return false;
        return pk.equals(other.pk)&&fk.equals(other.fk);
    }  
	
	@Override  
    public int hashCode()  
    {  
        final int prime = 31;  
        int result = 1;  
        result = prime * result + ((pk == null) ? 0 : pk.hashCode());  
        result = prime * result + ((fk == null) ? 0 : fk.hashCode());    
        return result;  
    } 

}
