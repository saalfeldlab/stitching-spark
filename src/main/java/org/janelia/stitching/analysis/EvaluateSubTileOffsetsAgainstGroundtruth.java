package org.janelia.stitching.analysis;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TilePair;
import org.janelia.stitching.Utils;
import org.janelia.util.Conversions;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import mpicbg.models.Model;

public class EvaluateSubTileOffsetsAgainstGroundtruth
{
	private static class EvaluateSubTileOffsetsAgainstGroundtruthCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-g", aliases = { "--groundtruthTilesPath" }, required = true,
				usage = "Path to the grountruth tile configuration file.")
		public String groundtruthTilesPath;

		@Option(name = "-p", aliases = { "--pairwiseConfigurationPath" }, required = true,
				usage = "Path to the pairwise file to be evaluated.")
		public String pairwiseConfigurationPath;

		@Option(name = "-t", aliases = { "--tileToInspect" }, required = true,
				usage = "Tile to inspect (will print offsets for subtile pairs containing this tile).")
		public int tileToInspect;

		public boolean parsedSuccessfully = false;

		public EvaluateSubTileOffsetsAgainstGroundtruthCmdArgs( final String... args ) throws IllegalArgumentException
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
		}
	}

	public static < M extends Model< M > > void main( final String[] args ) throws Exception
	{
		final EvaluateSubTileOffsetsAgainstGroundtruthCmdArgs parsedArgs = new EvaluateSubTileOffsetsAgainstGroundtruthCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();
		final Map< Integer, TileInfo > groundtruthTilesMap = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( parsedArgs.groundtruthTilesPath ) ) ) );
		final List< SerializablePairWiseStitchingResult > pairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( parsedArgs.pairwiseConfigurationPath ) ) );

		final List< SerializablePairWiseStitchingResult > pairwiseShiftsContainingRequestedTile = new ArrayList<>();
		for ( final SerializablePairWiseStitchingResult pairwiseShift : pairwiseShifts )
			if ( pairwiseShift.getSubTilePair().getA().getFullTile().getIndex().intValue() == parsedArgs.tileToInspect || pairwiseShift.getSubTilePair().getB().getFullTile().getIndex().intValue() == parsedArgs.tileToInspect )
				pairwiseShiftsContainingRequestedTile.add( pairwiseShift );

		if ( pairwiseShiftsContainingRequestedTile.isEmpty() )
			throw new RuntimeException( "no subTile pairs containing tile " + parsedArgs.tileToInspect + " were found" );

		System.out.println( "Found " + pairwiseShiftsContainingRequestedTile.size() + " subtile pairs containing tile " + parsedArgs.tileToInspect + System.lineSeparator() );

		final Map< Integer, TileInfo > fullTilesInMatches = new TreeMap<>();

		for ( final SerializablePairWiseStitchingResult pairwiseShift : pairwiseShiftsContainingRequestedTile )
		{
			final double[] stageSubTileOffset = new double[ pairwiseShift.getNumDimensions() ];
			for ( int d = 0; d < stageSubTileOffset.length; ++d )
				stageSubTileOffset[ d ] = pairwiseShift.getSubTilePair().getB().getFullTile().getStagePosition( d ) - pairwiseShift.getSubTilePair().getA().getFullTile().getStagePosition( d ) + pairwiseShift.getSubTilePair().getB().realMin( d ) - pairwiseShift.getSubTilePair().getA().realMin( d );

			final double[] groundtruthSubTileOffset = new double[ pairwiseShift.getNumDimensions() ];
			final TilePair groundtruthTilePair = new TilePair( groundtruthTilesMap.get( pairwiseShift.getSubTilePair().getA().getFullTile().getIndex() ), groundtruthTilesMap.get( pairwiseShift.getSubTilePair().getB().getFullTile().getIndex() ) );
			for ( int d = 0; d < groundtruthSubTileOffset.length; ++d )
				groundtruthSubTileOffset[ d ] = groundtruthTilePair.getB().getStagePosition( d ) - groundtruthTilePair.getA().getStagePosition( d ) + pairwiseShift.getSubTilePair().getB().realMin( d ) - pairwiseShift.getSubTilePair().getA().realMin( d );

			System.out.println( String.format(
					"  subtile pair %s: stage offset: %s, groundtruth offset=%s, estimated offset=%s",
					pairwiseShift.getSubTilePair(),
					Arrays.toString( round( stageSubTileOffset ) ),
					Arrays.toString( round( groundtruthSubTileOffset ) ),
					Arrays.toString( round( Conversions.toDoubleArray( pairwiseShift.getOffset() ) ) )
				) );

			for ( final TileInfo fullTile : pairwiseShift.getSubTilePair().getFullTilePair().toArray() )
				fullTilesInMatches.put( fullTile.getIndex(), fullTile );
		}

		System.out.println( System.lineSeparator() + String.format( "Requested full tile info: %d (%s)", parsedArgs.tileToInspect, Utils.getTileCoordinatesString( fullTilesInMatches.get( parsedArgs.tileToInspect ) ) ) );
		System.out.println( "Full tiles in pairwise matches:" );
		for ( final TileInfo fullTile : fullTilesInMatches.values() )
			if ( fullTile.getIndex().intValue() != parsedArgs.tileToInspect )
				System.out.println( "  " + String.format( "%d (%s)", fullTile.getIndex(), Utils.getTileCoordinatesString( fullTile ) ) );
	}

	private static long[] round( final double[] arr )
	{
		return Arrays.stream( arr ).mapToLong( Math::round ).toArray();
	}
}
