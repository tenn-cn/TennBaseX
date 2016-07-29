package tenndb.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tenndb.dm.UUID;
import tenndb.im.IBTree;



public class RIBTree implements IBTree {

	protected final int size;
	protected BTNode root;
	
	protected Map<Long, BTNode> map;
	protected List<BTNode> list;
	
	protected ReadWriteLock lock = new ReentrantReadWriteLock(false);
	
	public void RLock()   { this.lock.readLock().lock();    }
	
	public void URLock()  { this.lock.readLock().unlock();  }

	public void WLock()   { this.lock.writeLock().lock();   }
	
	public void UWLock()  { this.lock.writeLock().unlock(); }

	
	public RIBTree() {
		super();
		this.root   = null;
		this.size   = BTNode.BALANCE_SIZE;
		this.map    = new HashMap<Long, BTNode>();
		this.list   = new ArrayList<BTNode>();
	}
	
	public RIBTree(BTNode root) {
		super();
		this.root   = root;
		this.size   = BTNode.BALANCE_SIZE;		
		this.map    = new HashMap<Long, BTNode>();
		this.list   = new ArrayList<BTNode>();
	}
	
	public final Map<Long, BTNode> getMap() {
		return map;
	}

	public final List<BTNode> getList() {
		return list;
	}

	public UUID getRoot() { return this.root.getUUID(); }
	
    public void print(List<String> strList){
    			
		try{
			this.RLock();			
			if( root != null ){
				root.toString(strList);
			}
		}catch(Exception e){}
		finally{
			this.URLock();
		}
    }
	
    public String toString(){
    	String str = null;
        	
		try{
	    	this.RLock();			
	    	if( root != null ){
	            str = root.toString();
	        }	
	    	
		}catch(Exception e){}
		finally{
			this.URLock();
		}
		
    	return str;
    }
    
    @Override
	public void printTreePrior(){
		
    	try{
        	this.RLock();
        	
    		if(null != root){				
				BTNode currentNode = root;
				while(null != currentNode && !currentNode.isLeaf()){
					currentNode = this.map.get(currentNode.getRawChild(currentNode.getNumberOfKeys()).getUID());
				}
				
				if(null != currentNode){
					int index = 0;
					String str = "";				
					while(null != currentNode){
						
						str = "";
						index++;
						for(int i = currentNode.getNumberOfKeys() - 1; i >= 0 ; --i){
							if(null != currentNode.getRawChild(i)){
								str += currentNode.getRawKey(i).getUID() + "_" + currentNode.getRawChild(i).getUID() + ",";		
							}else{
								str += currentNode.getRawKey(i).getUID() + "_null,";
							}
						}
//						System.out.println(index + ":" + str);
						currentNode = this.map.get(currentNode.getPrior().getUID()); 
					}
				}			
			}
		}catch(Exception e){}
		finally{
			this.URLock();
		}			
	}
	
	@Override
	public void printNext() {
		
		try{			
			this.RLock();
			
			if(null != root){
				BTNode currentNode = root;
				while(null != currentNode && !currentNode.isLeaf()){
					currentNode = this.map.get(currentNode.getRawChild(0).getUID());				
				}
				
				if(null != currentNode){
					int index = 0;
					String str = "";				
	
					while(null != currentNode){
						str = "PageID = " + currentNode.getUUID().getUID() + ", ";
						index++;
						for(int i = 0; i < currentNode.getNumberOfKeys(); ++i){
							if(null != currentNode.getRawChild(i)){
								str += currentNode.getRawKey(i).getUID() + "_" + currentNode.getRawChild(i).getUID() + ",";								
							}else{
								str += currentNode.getRawKey(i).getUID() + "_null,";
							}
						}
//						System.out.println(index + ":" + str);
						
						UUID uid = currentNode.getNext();
						currentNode = this.map.get(uid.getUID()); 
					}
				}
			}
		}catch(Exception e){}
		finally{
			this.URLock();
		}		
	}
	
	protected boolean addChild(BTNode parentNode, UUID key, BTNode newNode){
		
		return parentNode.addChild(key, newNode);
	}

	@Override
	public boolean insert(UUID key, UUID value){
		boolean b = false;		
		try{			
			this.RLock();
			
			if(null!= root){
				BTNode currentNode = root;

				while(null != currentNode && !currentNode.isLeaf()){
					UUID uid = currentNode.getChild(key);
					currentNode = this.map.get(uid.getUID());
				}

				if(null != currentNode){
					BTNode leaf = currentNode;
					
					boolean inserted = leaf.addValue(key, value);

					if(!inserted){

						this.URLock(); // read lock up to write lock
						this.WLock();

						BTNode newRight = leaf.splitLeaf(key, value);

						this.map.put(newRight.getUUID().getUID(), newRight);
						this.list.add(newRight);
						
						BTNode parent     = map.get(newRight.getParent());
						
						UUID addToParent  = newRight.lowerBound();
						
						while(null != parent && ! this.addChild(parent,addToParent, newRight)){

							BTNode parentRight = parent.splitBranch(addToParent, newRight); 	

							this.map.put(parentRight.getUUID().getUID(), parentRight);
							this.list.add(parentRight);
							
		                    addToParent = parent.middleKey();
							parent      = this.map.get(parent.getParent().getUID());
							newRight    = parentRight;
						}

						if(null == parent){
							
							BTNode newRoot = new BTNode(this, root.getUUID(), newRight.getUUID(), addToParent);

							this.map.put(newRoot.getUUID().getUID(), newRoot);

							this.list.add(newRoot);
							
							root.setParent(newRoot.getUUID());
							newRight.setParent(newRoot.getUUID());
							root        = newRoot;					
						}	

						this.RLock(); // write lock down to read lock
						this.UWLock();						
					}		
				}
			}else{				
				this.URLock();
				this.WLock();
				
				this.root = new BTNode(this, key, value);

				this.map.put(this.root.getUUID().getUID(), this.root);
				this.list.add(this.root);
				
				this.RLock();
				this.UWLock();
			}
						
		}catch(Exception e){
			System.out.println(e);
		}
		finally{
			this.URLock();
		}	
		
		return b;
	}

	@Override
	public UUID update(UUID key, UUID rowData) {
		UUID oldObj = null;
		
		try{
			this.RLock();
			
			BTNode currentNode = this.root;
			while(null != currentNode && !currentNode.isLeaf()){
				currentNode = this.map.get(currentNode.getChild(key).getUID());
			}
			
			if(null != currentNode){
				oldObj = currentNode.setValue(key, rowData);
			}
			
			//		this.indexMgr.flushNewPages();
		}catch(Exception e){}
		finally{
			this.URLock();
		}	
		
		return oldObj;
	}

/*	@Override
	public IndexBlock delete(int key) {
		IndexBlock oldObj = null;
		
		try{
			this.WLock();
			
			BTreeNode currentNode = root;
			while(null != currentNode && !currentNode.isLeaf){
				currentNode = currentNode.getChild(key);
			}
			
			if(null != currentNode){
				oldObj = currentNode.delValue(key);
				if(null != oldObj){
//					this.indexMgr.appendNewIndexPage(null, currentNode);
				}
			}
			
	//		this.indexMgr.flushNewPages();
		}catch(Exception e){}
		finally{
			this.UWLock();
		}	
		
		return oldObj;
	}*/
	
	@Override
	public BTNode seekNode(UUID key) {
		BTNode currentNode = null;
		
		try{
			this.RLock();
			
			currentNode = this.root;
			
			while(null != currentNode && !currentNode.isLeaf()){
				currentNode = this.map.get(currentNode.getChild(key).getUID());
			}
		}catch(Exception e){}
		finally{
			this.URLock();
		}
		return currentNode;
	}
		
	@Override
	public UUID search(UUID key) {
		UUID value = null;
	
		try{
			this.RLock();
			
			BTNode currentNode = this.root;
			while(null != currentNode && !currentNode.isLeaf()){
				currentNode = this.map.get(currentNode.getChild(key).getUID());
			}
			
			if(null != currentNode && currentNode.isLeaf()){
				value = currentNode.getValue(key);
			}
		}catch(Exception e){}
		finally{
			this.URLock();
		}
		
		return value;
	}

	@Override
	public List<UUID> range(UUID fromKey, UUID toKey, boolean isInc) {
		List<UUID> list = new ArrayList<UUID>();
		
		try{	
			this.RLock();			
			if(fromKey.getUID() <= toKey.getUID()){
				
				if(isInc){			
					BTNode node = this.seekNode(fromKey);
					boolean next = true;
					while(null != node){
						int i = 0;
						for(; i < node.getNumberOfKeys(); ++i){
							UUID uid = node.getRawKey(i);
							if(uid.getUID() >= fromKey.getUID() && uid.getUID() <= toKey.getUID()){								
										
								UUID value = node.getRawChild(i);
								list.add(value);
								
							}else if(uid.getUID() > toKey.getUID()){
								next = false;
								break;
							}
						}
						if(next){
							node = this.map.get(node.getNext().getUID());
						}
						else
							node = null;
					}			
				}else{
					
					BTNode node = this.seekNode(toKey);
					boolean next = true;
					while(null != node){
						int i = node.getNumberOfKeys() - 1;
						for(; i >= 0; --i){
							UUID uid = node.getRawKey(i);
							if(uid.getUID() >= fromKey.getUID() && uid.getUID() <= toKey.getUID()){	
								UUID value = node.getRawChild(i);
								list.add(value);
							}else if(uid.getUID() < fromKey.getUID()){
								next = false;
								break;
							}
						}
						if(next){
							node = this.map.get(node.getPrior().getUID());
						}
						else
							node = null;
					}
				}
			}
		}catch(Exception e){}
		finally{
			this.URLock();
		}
		
		return list;
	}

		
	@Override
	public int count(UUID fromKey, UUID toKey) {
		int count = 0;
		
		try{	
			this.RLock();			
			if(fromKey.getUID() <= toKey.getUID()){
				
				BTNode node = this.seekNode(fromKey);
				boolean next = true;
				while(null != node){
					int i = 0;
					for(; i < node.getNumberOfKeys(); ++i){
						if(node.getRawKey(i).getUID() >= fromKey.getUID() 
						&& node.getRawKey(i).getUID() <= toKey.getUID()){
							++count;							
						}else if(node.getRawKey(i).getUID() > toKey.getUID()){
							next = false;
							break;
						}
					}
					if(next){
						node = this.map.get(node.getNext().getUID());
					}
					else
						node = null;
				}				
			}
		}catch(Exception e){}
		finally{
			this.URLock();
		}
		
		return count;
	}

}
