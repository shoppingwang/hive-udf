package com.gochinatv.hive.common;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * 将IP地址转换为城市信息
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2016/3/31 11:03
 */
public class IPToLocation extends UDF
{
    private static final String IP_REGEX = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";

    private static final Pattern IP_PATTERN = Pattern.compile(IP_REGEX);

    private static DatabaseReader reader = null;

    private static final int TRANSFORM_TYPE_COUNTRY = 1;

    private static final int TRANSFORM_TYPE_CITY = 2;

    private static final int GZIP_BUFFER_SIZE = 1024;

    static
    {
        InputStream sourceInputStream = IPToLocation.class.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb.gz");
        GZIPInputStream gzipInputStream = null;

        try
        {
            gzipInputStream = new GZIPInputStream(sourceInputStream, GZIP_BUFFER_SIZE);
            reader = new DatabaseReader.Builder(gzipInputStream).build();
        }
        catch (IOException e)
        {
            e.printStackTrace();

            try
            {
                if (null != reader)
                {
                    reader.close();
                }

                if (null != gzipInputStream)
                {
                    gzipInputStream.close();
                }

                sourceInputStream.close();
            }
            catch (IOException ignored)
            {
            }

            throw new RuntimeException(e);
        }
    }

    private String toCountry(String ipAddr) throws IOException, GeoIp2Exception
    {
        InetAddress ipAddress = InetAddress.getByName(ipAddr);

        CityResponse response = reader.city(ipAddress);
        Country country = response.getCountry();

        return country.getNames().get("zh-CN");
    }

    public String evaluate(String ipAddr, int type)
    {
        if (null == ipAddr)
        {
            return null;
        }
        else if (isIPAddr(ipAddr))
        {
            try
            {
                if (TRANSFORM_TYPE_COUNTRY == type)
                {
                    return toCountry(ipAddr);
                }
                else if (TRANSFORM_TYPE_CITY == type)
                {
                    return toCity(ipAddr);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        return ipAddr;
    }

    private String toCity(String ipAddr) throws IOException, GeoIp2Exception
    {
        InetAddress ipAddress = InetAddress.getByName(ipAddr);

        CityResponse response = reader.city(ipAddress);
        City city = response.getCity();

        return city.getNames().get("zh-CN");
    }

    private boolean isIPAddr(String ipAddr)
    {
        return IP_PATTERN.matcher(ipAddr).find();
    }

    public static void main(String[] args) throws Exception
    {
        IPToLocation ipToLocation = new IPToLocation();

        System.out.println(ipToLocation.evaluate("211.103.136.210", 1));
        System.out.println(ipToLocation.evaluate("211.103.136.210", 2));

        System.out.println(ipToLocation.evaluate("128.101.101.101", 1));
        System.out.println(ipToLocation.evaluate("128.101.101.101", 2));
    }
}
