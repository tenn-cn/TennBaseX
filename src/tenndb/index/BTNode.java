package tenndb.index;

import java.util.List;

import tenndb.common.ByteUtil;
import tenndb.dm.UUID;

public class BTNode {

	public static int INC = 1;
	
	protected final static int  BALANCE_SIZE = 32;
	protected final static byte NODE_BRANCH  = 0; 
	protected final static byte NODE_LEAF    = 1;
	protected final static byte NODE_ROOT    = 2;
	
	public final static int _IS_LEAF_OFFSET   = 0;
	public final static int _NO_KEYS_OFFSET   = 1;
	public final static int _UUID_OFFSET      = 2;
	public final static int _PARENT_OFFSET    = _UUID_OFFSET   + UUID.LEN_UUID;
	public final static int _LEFT_OFFSET      = _PARENT_OFFSET + UUID.LEN_UUID;
	public final static int _RIGHT_OFFSET     = _LEFT_OFFSET   + UUID.LEN_UUID;
	public final static int NODE_HEADER_SIZE  = _RIGHT_OFFSET  + UUID.LEN_UUID;
	public final static int _NODE_SIZE        = NODE_HEADER_SIZE + (2*UUID.LEN_UUID)*(BALANCE_SIZE*2+2);
	
/*
	node的二进制结构如下:
	[Leaf Flag  ] byte
	[No Of keys ] byte
	[Node   UUID] UUID 8 byte
	[Parent UUID] UUID 8 byte
	[Left   UUID] UUID 8 byte
	[Right  UUID] UUID 8 byte
	
    [Key0][Son0]  [Key1][Son1]  ... [KeyN][SonN] (when )or
    [Key0][blk0]  [Key1][blk1]  ... [KeyN][blkN] 	
 */
	
	protected byte[] raw;
	
	protected BTNode prior;
	protected BTNode next;
	protected BTNode parent;
	
	protected RIBTree tree;
	
	public BTNode(RIBTree tree){
		this.tree = tree;
		this.raw  = new byte[_NODE_SIZE];
		this.setUUID(INC++);
	}
	
	public BTNode(RIBTree tree, UUID left, UUID right, UUID key){		
		this.tree = tree;
		this.raw = new byte[_NODE_SIZE];
		this.setUUID(INC++);
		this.setNumberOfKeys((short)1);
		this.setRawKey(0, key);
		this.setNodeType(NODE_BRANCH);
		this.setRawChild(0, left);
		this.setRawChild(1, right);
	}
	
	public BTNode(RIBTree tree, UUID key, UUID obj){
		this.tree = tree;
		this.raw = new byte[_NODE_SIZE];
		this.setUUID(INC++);
		this.setNumberOfKeys((short)1);
		this.setRawKey(0, key);
		this.setRawChild(0, obj);
		this.setNodeType(NODE_LEAF);
	}
	
	public void toString(List<String> strList){
				
		if(!this.isLeaf()){
			if(null != this.getRawChild(0)){
			   BTNode node = this.tree.map.get(this.getRawChild(0).getUID());
			   node.toString(strList);  
			   
		       for( int i = 0; i < this.getNumberOfKeys(); ++i ){
		    	   node = this.tree.map.get(this.getRawChild(i+1).getUID());
		    	   node.toString(strList); 
		       }
			}
		}else{
			String output = "[L";
	        for( int i = 0; i < this.getNumberOfKeys(); ++i ){
	            output += " " + this.getRawKey(i).getUID() + ":" + this.getRawChild(i).getUID() + ", ";
	        }
	        
			if(null != next){
				output += "next = "+ this.next.getRawKey(0).getUID() +": ";
			}
			output += "]";
			strList.add(output);
		}	
	}
	

	public String toString(){
		String output = "[L";
		
//		this.lockRead();

		if(!this.isLeaf()){
			if(null != this.getRawChild(0)){
			   BTNode node = this.tree.map.get(this.getRawChild(0).getUID());
		       output += "[" + node.toString() + "], ";  
		        for( int i = 0; i < this.getNumberOfKeys(); ++i ){
		            output += this.getRawKey(i).getUID() + ":" + this.tree.map.get(this.getRawChild(i+1).getUID()).toString(); 
		            if( i < this.getNumberOfKeys() - 1 ) output += ", ";
		        }
			}
		}else{

	        for( int i = 0; i < this.getNumberOfKeys(); ++i ){
	            output += " " + this.getRawKey(i).getUID() + ":" + this.tree.map.get(this.getRawChild(i).getUID()).toString() + ", ";
	        }
	        
			if(null != next)
				 output += "next = "+ next.getRawKey(0).getUID() +": ";  
		}	
		
//		this.unLockRead();
		
		return output + "]";
	}
	
	
	public UUID getParent(){
		return new UUID(ByteUtil.byte8ToLong_big(this.raw, _PARENT_OFFSET));
	}
	
	public void setParent(UUID key){
		ByteUtil.longToByte8_big(this.raw, _PARENT_OFFSET, key.getUID());
	}
	
	public UUID getPrior(){
		return new UUID(ByteUtil.byte8ToLong_big(this.raw, _LEFT_OFFSET));
	}
	
	public void setPrior(UUID key){
		ByteUtil.longToByte8_big(this.raw, _LEFT_OFFSET, key.getUID());
	}
	
	public UUID getNext(){
		return new UUID(ByteUtil.byte8ToLong_big(this.raw, _RIGHT_OFFSET));
	}
	
	public void setNext(UUID key){
		ByteUtil.longToByte8_big(this.raw, _RIGHT_OFFSET, key.getUID());
	}
	
	public void setNodeType(byte type){
		this.raw[_IS_LEAF_OFFSET] = type;
	}
	
	public byte getNodeType(){
		return this.raw[_IS_LEAF_OFFSET];
	}
	
	public boolean isLeaf(){
		return NODE_LEAF == this.raw[_IS_LEAF_OFFSET];
	}
	
	public boolean isRoot(){
		return NODE_ROOT == this.raw[_IS_LEAF_OFFSET];
	}
	
	public boolean isBranch(){
		return NODE_BRANCH == this.raw[_IS_LEAF_OFFSET];
	}
	
	public short getNumberOfKeys(){
		return this.raw[_NO_KEYS_OFFSET];
	}
	
	public void setNumberOfKeys(short number){
		this.raw[_NO_KEYS_OFFSET] = ByteUtil.shortToByte(number);
	}
	
	public UUID getUUID(){
		return new UUID(ByteUtil.byte8ToLong_big(this.raw, _UUID_OFFSET));
	}
	
	public void setUUID(long uid){
		ByteUtil.longToByte8_big(this.raw, _UUID_OFFSET, uid);
	}
	
	public UUID getRawKey(int index){
		int offset = NODE_HEADER_SIZE + index*(UUID.LEN_UUID*2);
		return new UUID(ByteUtil.byte8ToLong_big(this.raw, offset));
	}
	
	public void setRawKey(int index, UUID key){
		int offset = NODE_HEADER_SIZE + index*(UUID.LEN_UUID*2);
		ByteUtil.longToByte8_big(this.raw, offset, key.getUID());
	}
	
	public UUID getRawChild(int index){
		int offset = NODE_HEADER_SIZE + index*(UUID.LEN_UUID*2) + UUID.LEN_UUID;
		return new UUID(ByteUtil.byte8ToLong_big(this.raw, offset));
	}
	
	public void setRawChild(int index, UUID uid){
		int offset = NODE_HEADER_SIZE + index*(UUID.LEN_UUID*2) + UUID.LEN_UUID;
		ByteUtil.longToByte8_big(this.raw, offset, uid.getUID());
	}
	
	public UUID middleKey(){
		return this.getRawKey(this.getNumberOfKeys()/2);
	}
	
	public UUID lowerBound(){
		return this.getRawKey(0);
	}
	
	public UUID upperBound(){
		return this.getRawKey(this.getNumberOfKeys()-1);
	}
	
	public void insertKeyAndValueToBranch(int index, int size, UUID key, UUID value){
		for(int t = size; t > index; --t){
			this.setRawKey(t, this.getRawKey(t - 1));
			this.setRawChild(t + 1, this.getRawChild(t));
		}
		
		this.setRawKey(index, key);
		this.setRawChild(index + 1, value);
	}
	
	public void copyKeyAndValue(BTNode src, int from, int to, BTNode dest){
		
		int newLength = to - from;
		for(int t = 0; t < newLength; ++t){
			if(t + from < to){
				dest.setRawKey(t, src.getRawKey(from + t));
				dest.setRawChild(t, src.getRawChild(from + t));
			}
		}
		dest.setRawChild(newLength, src.getRawChild(from + newLength));
	}
	
	public BTNode splitBranch(UUID key, BTNode node){
		BTNode newNode = null;
		
		try{
			if(!this.isLeaf()){

				if(this.getNumberOfKeys() == BALANCE_SIZE){

					int i = 0;
					for(; i < BALANCE_SIZE;){
						if(key.getUID() >= this.getRawKey(i).getUID())
							++i;
						else
							break;
					}

					if(this.getRawKey(i).getUID() != key.getUID()){			
						
						insertKeyAndValueToBranch(i, BALANCE_SIZE, key, node.getUUID());

						newNode = new BTNode(this.tree);
						
						copyKeyAndValue(this, (BALANCE_SIZE + 1)/2 + 1, BALANCE_SIZE + 1, newNode);
						
						newNode.setNodeType(NODE_BRANCH);
						newNode.setNumberOfKeys((short) ((BALANCE_SIZE + 1)/2));
						
						newNode.setPrior(this.getUUID());
						newNode.setNext(this.getNext());
						newNode.setParent(this.getParent());
						
						this.setNumberOfKeys((short) ((BALANCE_SIZE + 1)/2));
						this.setNext(newNode.getUUID());
					}				
				}
			}
		}catch(Exception e){}
		finally{

		}
		
		return newNode;
	}
	
	public BTNode splitLeaf(UUID key, UUID value){

		BTNode newRight = null;
		
		try{
			
			if(this.isLeaf()){
				
				if(key.getUID() > this.upperBound().getUID()){

					BTNode newNode = new BTNode(this.tree);
					newNode.setNodeType(NODE_LEAF);
					newNode.setNumberOfKeys((short) 1);
					copyKeyAndValue(this, BALANCE_SIZE - 1, BALANCE_SIZE, newNode);

					newNode.setParent(this.getParent());					
					newNode.setPrior(this.getUUID());
					newNode.setNext(this.getNext());
					
					newRight = newNode;
					this.setNext(newNode.getUUID());
					this.setNumberOfKeys((short) (BALANCE_SIZE - 1));
					
					if(key.getUID() >= newRight.lowerBound().getUID()){
					    newRight.addValue(key, value);
					}else{
						this.addValue(key, value);
					}
				}else{

					BTNode newNode = new BTNode(this.tree);
					newNode.setNodeType(NODE_LEAF);
					newNode.setNumberOfKeys((short) 1);
					copyKeyAndValue(this, BALANCE_SIZE / 2, BALANCE_SIZE, newNode);

					newNode.setParent(this.getParent());					
					newNode.setPrior(this.getUUID());
					newNode.setNext(this.getNext());
					
					newRight = newNode;
					this.setNext(newNode.getUUID());
					this.setNumberOfKeys((short) (BALANCE_SIZE / 2));
					
					if(key.getUID() >= newRight.lowerBound().getUID()){
						newRight.addValue(key, value);
					}else{
						this.addValue(key, value);
					}
				}				
			}
		}catch(Exception e){}
		finally{

		}

		return newRight;
	}
	
	public UUID setValue(UUID key, UUID newObj) {
		UUID oldObj = null;
				
		try{			

			if(this.isLeaf()){
				int i = 0;
				for(; i < this.getNumberOfKeys(); ){
					if(this.getRawKey(i).getUID() < key.getUID())
						++i;
					else
						break;
				}
				
				if(this.getRawKey(i).getUID() == key.getUID()){					
					oldObj = this.getRawChild(i);
					this.setRawChild(i, newObj);
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}finally{
		}
		
		return oldObj;
	}
	
	public boolean addChild(UUID key, BTNode node){
		boolean b = false;

		try{
			if(!this.isLeaf() && this.getNumberOfKeys() < BALANCE_SIZE){
				int i = 0;
				for(; i < this.getNumberOfKeys(); ){
					if(this.getRawKey(i).getUID() < key.getUID())
						++i;
					else
						break;
				}
				
				if(i < BALANCE_SIZE){
					
					for(int t = this.getNumberOfKeys(); t > i; --t){
						this.setRawKey(t, this.getRawKey(t - 1));
						this.setRawChild(t+1, this.getRawChild(t));
					}
					this.setRawKey(i, key);
					this.setRawChild(i+1, node.getUUID());
					
					this.setNumberOfKeys((short) (this.getNumberOfKeys() + 1));
					
					b = true;
				}
			}
		}catch(Exception e){}
		finally{

		}

		return b;
	}
	
	public boolean addValue(UUID key, UUID newValue){

		boolean b = false;
		
		try{
			if(this.isLeaf()){
				int i = 0;
				for(; i < this.getNumberOfKeys(); ){
					if(this.getRawKey(i).getUID() < key.getUID())
						++i;
					else
						break;
				}
				
				if(i != this.getNumberOfKeys() && this.getRawKey(i).getUID() == key.getUID()){
					b = false;
				}else if(this.getNumberOfKeys() != BALANCE_SIZE){
					
					for(int t = this.getNumberOfKeys(); t > i; --t){
						this.setRawKey(t, this.getRawKey(t-1));
						this.setRawChild(t, this.getRawChild(t-1));
					}
								
					this.setRawKey(i, key);
					this.setRawChild(i, newValue);
					this.setNumberOfKeys((short) (this.getNumberOfKeys()+1));
					b = true;
				}
			}			
		}catch(Exception e){}
		finally{
		
		}

		return b;
	}
	
	public UUID getChild(UUID key){
		UUID child = null;
		
		try{
			
			if(!this.isLeaf()){
				int i = 0;
				for(; i < this.getNumberOfKeys(); ){
					if(this.getRawKey(i).getUID() <= key.getUID())
						++i;
					else
						break;
				}
				child = this.getRawChild(i);
			}
		}
		catch(Exception e){
			System.out.println(e);
		}
		finally{

		}
		
		return child;
	}
	
	
	public UUID getValue(UUID key){
		UUID value = null;
				
		try{
		
			if(this.isLeaf()){
				int i = 0;
				for(; i < this.getNumberOfKeys(); ){
					if(this.getRawKey(i).getUID() < key.getUID())
						++i;
					else
						break;
				}
				
				if(this.getRawKey(i).getUID() == key.getUID())
				{
					value = this.getRawChild(i);
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
		finally{

		}
		return value;
	}
}
