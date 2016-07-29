package tenndb.dm;

public class UUID {

	public static final long INF  = (1 << 63) - 1 + (1 << 63);
	public static final long NilUUID = 0;
	
	protected long uid;
	
	public UUID(long uid){
		this.uid = uid;
	}
	
	public long getUID(){
		return this.uid;
	}
	
	public static final int LEN_UUID = 8;
	
	public static PAddress UUID2Address(UUID uid)
	{
		PAddress address = new PAddress();
		
		address.offset = (int) (uid.uid & ((1 << 16) - 1));
		
		return address;
	}


}
