package cn.whbing.spark.SparkApps.cores;

import java.io.Serializable;

import scala.math.Ordered;

/*
 * 自定义二次排序，实现的是scala的接口，不是java中的排序接口
 * */

public class SecondSortKey implements Ordered<SecondSortKey>,Serializable{
	
	//需要二次排序的key
	private int first;
	private int second;
	
		
	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	public SecondSortKey(int first,int second) {
		this.first = first;
		this.second = second;
	}
	
	@Override
	public boolean $greater(SecondSortKey other) {
		// 大于的时候的情况
		if(this.first > other.getFirst()){
			return true;
		}else if(this.first == other.getFirst() && this.second > other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(SecondSortKey other) {
		// 大于等于的情况
		if(this.$greater(other)){
			return true;
		}else if(this.first == other.getFirst() && this.second == other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SecondSortKey other) {
		// 小于的情况
		if(this.first < other.getFirst()){
			return true;
		}else if(this.first == other.getFirst() && this.second < other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(SecondSortKey other) {
		// TODO 小于等于的情况
		if(this.$less(other)){
			return true;
		}else if(this.first == other.getSecond() && this.second == other.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public int compare(SecondSortKey other) {
		if(this.first - other.getFirst() !=0){
			return this.first - other.getFirst();
		}else {
			return this.second - other.getSecond();
		}
	}

	@Override
	public int compareTo(SecondSortKey other) {
		if(this.first - other.getFirst() !=0){
			return this.first - other.getFirst();
		}else {
			return this.second - other.getSecond();
		} 
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondSortKey other = (SecondSortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	


}
