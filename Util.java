package LAB5;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class Util {
	public static void main ( String args [] ) 
	{
		//System.out.println( getHash("abccc") ) ;
	}
	public static String getHash ( String input )
	{
		try {
			byte[] result = MessageDigest.getInstance("MD5").digest(input.getBytes("UTF-8"));
			StringBuffer hexString = new StringBuffer();
			for ( int i = 0 ; i < result.length ; i++ )
			{
				String hex = Integer.toHexString(0xFF & result[i]);
				if (hex.length() == 1) {
				    // could use a for loop, but we're only dealing with a single byte
				    hexString.append('0');
				}
				hexString.append(hex);
			}
			return hexString.toString().substring(24);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null ;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null ;
		}
		
	}
	public static List<String> PermuteWords(String s)
	{
	    String[] ss = s.split(" ");
	    boolean[] used = new boolean[ss.length];
	    String res = "";
	    List<String> list = new ArrayList<String>();
	    permute(ss, used, res, 0, list);
	    return list;
	}

	private static void permute(String[] ss, boolean[] used, String res, int level, List<String> list)
	{
	    if (level == ss.length && res != "")
	    {
	        list.add(res);
	        return;
	    }
	    for (int i = 0; i < ss.length; i++)
	    {
	        if (used[i]) continue;
	        used[i] = true;
	        permute(ss, used, res + " " + ss[i], level + 1, list);
	        used[i] = false;
	    }
	}
}
