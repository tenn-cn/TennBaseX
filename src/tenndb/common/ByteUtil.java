package tenndb.common;

public final class ByteUtil {

	public static final int INT_SIZE   = Integer.SIZE / Byte.SIZE;
	public static final int SHORT_SIZE = Short.SIZE   / Byte.SIZE;
	public static final int BYTE_SIZE  = Byte.SIZE    / Byte.SIZE;
	
	public final static short BYTE_MAX_VALUE  = 256;
	public final static int   SHORT_MAX_VALUE = 65535;
	public final static long  INT_MAX_VALUE   = 4294967296L;
	
	public final static int PAGE_SIZE = 1 << 13;
	
	public final static byte[] longToByte8_big(long n)
	{
		byte[] buf = new byte[8];
		
		for(int i = 7; i >= 0; i--){
			buf[i] = (byte) (n & 0xFF);
			n >>= 8;
		}
		
		return buf;
	}
	
	public final static void longToByte8_big(byte[] buf, int offset, long n)
	{
		if(offset >= 0 && null != buf && buf.length >= (offset + 8))
		{
			for(int i = 7; i >= 0; i--){
				buf[i + offset] = (byte) (n & 0xFF);
				n >>= 8;
			}
		}
	}
	
	public final static long byte8ToLong_big(byte[] buf, int offset)
	{
		long n = 0;
		if(null != buf)
		{
			for(int i = 0 ;i < 8; i++)
			{
			    n <<= 8;
			    n |= (buf[offset + i] & 0x00000000000000FF);
			}
		}
		return n;
	}
	
	public final static short byteToShort(byte[] buf, int offset)
	{
		short n = 0;
		n = (short) (buf[offset] & 0x00FF);
		return n;
	}
	
	public final static byte shortToByte(short n)
	{
		byte b = 0;
		b = (byte) (n & 0x00FF);
		return b;
	}
	
	public final static int byte2ToShort_big(byte[] buf, int offset)
	{
		int n = 0;
		if(null != buf)
		{
			for(int i = 0 ;i < 2; i++)
			{
			    n <<= 8;
			    n |= (buf[offset + i] & 0x000000FF);
			}
		}
		return n;
	}
	
	public static byte[] shortToByte2_big(int n)
	{
		byte[] buf = new byte[2];
		
		for(int i = 1; i >= 0; i--){
			buf[i] = (byte) (n & 0xFF);
			n >>= 8;
		}
		
		return buf;
	}
	
	
	public final static void shortToByte2_big(byte[] buf, int offset, int n)
	{
		if(offset >= 0 && null != buf && buf.length >= (offset + 2))
		{
			for(int i = 1; i >= 0; i--){
				buf[i + offset] = (byte) (n & 0xFF);
				n >>= 8;
			}
		}
	}
	
	public final static int byte4ToInt_big(byte[] buf, int offset)
	{
		int n = 0;
		if(null != buf)
		{
			for(int i = 0 ;i < 4; i++)
			{
			    n <<= 8;
			    n |= (buf[offset + i] & 0x000000FF);
			}
		}
		return n;
	}
	
	public static byte[] intToByte4_big(int n)
	{
		byte[] buf = new byte[4];
		
		for(int i = 3; i >= 0; i--)
		{
			buf[i] = (byte) (n & 0xFF);
			n >>= 8;
		}
		
		return buf;
	}
	
	public final static void intToByte4_big(byte[] buf, int offset, int n)
	{
		if(offset >= 0 && null != buf && buf.length >= (offset + 4))
		{
			for(int i = 3; i >= 0; i--)
			{
				buf[i + offset] = (byte) (n & 0xFF);
				n >>= 8;
			}
		}
	}
}
