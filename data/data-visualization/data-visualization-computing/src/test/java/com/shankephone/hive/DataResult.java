package com.shankephone.hive;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import scala.runtime.AbstractFunction1;

public class DataResult extends AbstractFunction1<ResultSet, List> implements Serializable {
	
	    public List apply(ResultSet rs) {
	    	try {
	    		List<OrderInfo> list = new ArrayList<>();
				do {
					OrderInfo order = new OrderInfo();
					order.setCity_code(rs.getString("t.city_code"));
					order.setOrder_no(rs.getString("t.order_id"));
					order.setOrder_type(rs.getInt("t.order_type"));
					list.add(order);
				} while(rs.next());
				return list;
			} catch (SQLException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
	    }
	    
}
