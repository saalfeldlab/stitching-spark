package org.janelia.flatfield;

import java.nio.file.Paths;
import java.util.List;

import org.janelia.dataaccess.CloudURI;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.Interval;
import net.imglib2.SerializableFinalInterval;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

/**
 * Command line arguments parser for flatfield correction.
 *
 * @author Igor Pisarev
 */

public class FlatfieldCorrectionArguments
{
	@Option(name = "-i", aliases = { "--input" }, required = true,
			usage = "Path/link to a tile configuration JSON file. Multiple configuration (channels) can be passed at once.")
	private List< String > inputChannelsPaths;

	@Option(name = "--crop", required = false,
			usage = "Crop interval in a form of xMin,yMin,zMin,xMax,yMax,zMax")
	private String cropMinMaxInterval = null;

	@Option(name = "-b", aliases = { "--bins" }, required = false,
			usage = "Number of inner bins to use (two extra outer bins will be added to store the tails of the distribution)")
	private int innerBins = 256;

	@Option(name = "-v", aliases = { "--backgroundValue" }, required = false,
			usage = "Background intensity value which will be subtracted from the data (one per input channel). If omitted, it will be estimated automatically (less reliable than if the background intensity is known).")
	private List< Double > backgroundIntensityValues = null;

	@Option(name = "--min", required = false,
			usage = "Min value of a histogram")
	private Double histMinValue = null;

	@Option(name = "--max", required = false,
			usage = "Max value of a histogram")
	private Double histMaxValue = null;

	@Option(name = "--2d", required = false,
			usage = "Estimate 2D flatfield (slices are used as additional data points)")
	private boolean use2D = false;

	@Option(name = "--qmin", aliases = { "--minQuantile" }, required = false,
			usage = "Quantile to determine min histogram value")
	private Double histMinQuantile;

	@Option(name = "--qmax", aliases = { "--maxQuantile" }, required = false,
			usage = "Quantile to determine max histogram value")
	private Double histMaxQuantile;

	private static final double defaultHistMinQuantile = 0.05;
	private static final double defaultHistMaxQuantile = 0.95;

	private boolean parsedSuccessfully = false;

	public FlatfieldCorrectionArguments( final String[] args ) throws CmdLineException
	{
		final CmdLineParser parser = new CmdLineParser( this );
		try
		{
			parser.parseArgument( args );
			parsedSuccessfully = true;
		}
		catch ( final CmdLineException e )
		{
			System.err.println( e.getMessage() );
			parser.printUsage( System.err );
		}

		// validate min/max pair args
		if ( ( histMinValue == null ) != ( histMaxValue == null ) )
			throw new IllegalArgumentException( "histogram min/max values should be either both specified or omitted (will be estimated in this case)" );

		if ( ( histMinQuantile == null ) != ( histMaxQuantile == null ) )
			throw new IllegalArgumentException( "histogram min/max quantiles should be either both specified or omitted (default values <0.05, 0.95> will be used in this case)" );

		if ( histMinValue != null && histMaxValue != null )
		{
			if ( histMinQuantile != null && histMaxQuantile != null )
				throw new IllegalArgumentException( "histogram min/max quantiles should not be specified when histogram min/max values are explicitly provided" );
		}
		else if ( histMinQuantile == null && histMaxQuantile == null )
		{
			histMinQuantile = defaultHistMinQuantile;
			histMaxQuantile = defaultHistMaxQuantile;
		}

		// make sure that inputTileConfigurations contains absolute file paths if running on a traditional filesystem
		for ( int i = 0; i < inputChannelsPaths.size(); ++i )
			if ( !CloudURI.isCloudURI( inputChannelsPaths.get( i ) ) )
				inputChannelsPaths.set( i, Paths.get( inputChannelsPaths.get( i ) ).toAbsolutePath().toString() );

		if ( backgroundIntensityValues != null && backgroundIntensityValues.size() != inputChannelsPaths.size() && backgroundIntensityValues.size() != 1 )
			throw new IllegalArgumentException( "Background intensity values should be provided for each input channel" );
	}

	public boolean parsedSuccessfully() { return parsedSuccessfully; }

	public List< String > inputChannelsPaths() { return inputChannelsPaths; }
	public String cropMinMaxIntervalStr() { return cropMinMaxInterval; };
	public boolean use2D() { return use2D; }
	public Pair< Double, Double > getMinMaxQuantiles() { return new ValuePair<>( histMinQuantile, histMaxQuantile ); }

	public HistogramSettings getHistogramSettings()
	{
		return new HistogramSettings(
				histMinValue,
				histMaxValue,
				innerBins + 2  // add two extra bins for tails of the distribution
			);
	}

	public Double backgroundIntensityValue( final int channel )
	{
		if ( backgroundIntensityValues == null )
			return null;
		else if ( backgroundIntensityValues.size() == 1 )
			return backgroundIntensityValues.get( 0 ); // keep backwards compatibility with the older usage (allow the same value for all input channels)
		else
			return backgroundIntensityValues.get( channel );
	}

	public Interval cropMinMaxInterval( final long[] fullTileSize )
	{
		if ( cropMinMaxInterval == null )
		{
			final long[] minSize = new long[ fullTileSize.length * 2 ];
			System.arraycopy( fullTileSize, 0, minSize, fullTileSize.length, fullTileSize.length );
			return SerializableFinalInterval.createMinSize( minSize );
		}

		final String[] intervalStrSplit = cropMinMaxInterval.trim().split(",");
		final long[] intervalMinMax = new long[ intervalStrSplit.length ];
		for ( int i = 0; i < intervalMinMax.length; i++ )
			intervalMinMax[ i ] = Long.parseLong( intervalStrSplit[ i ] );
		return SerializableFinalInterval.createMinMax( intervalMinMax );
	}
}
