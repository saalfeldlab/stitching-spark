package org.janelia.stitching.analysis;

import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;

@Deprecated
public class TracePairwiseShifts
{
	public static void main( final String[] args ) throws Exception
	{
		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final Map< Integer, TileInfo >[] channelsMap = new TreeMap[ args.length - 2 ];
		for ( int i = 0; i < channelsMap.length; i++ )
			channelsMap[ i ] = Utils.createTilesMap( TileInfoJSONProvider.loadTilesConfiguration( dataProvider.getJsonReader( URI.create( args[ i ] ) ) ) );

		for ( int i = 1; i < channelsMap.length; i++ )
			if ( channelsMap[ i ].size() != channelsMap[ i - 1 ].size() )
				throw new Exception( "Different number of tiles per channel" );
		final int tilesPerChannel = channelsMap.length > 0 ? channelsMap[ 0 ].size() : 0;
		if ( tilesPerChannel > 0 )
			System.out.println( tilesPerChannel + " tiles per channel" );

		final String pairwiseShiftsFilepath = args[ args.length - 2 ];
		final List< SerializablePairWiseStitchingResult > shifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( pairwiseShiftsFilepath ) ) );

		final String outputFolder = args[ args.length - 1 ];

		final String suffixInliers = ( pairwiseShiftsFilepath.contains( "_inliers" ) ? "_inliers" : "" );
		final String suffixDim = ( pairwiseShiftsFilepath.contains( "-x" ) ? "-x" : ( pairwiseShiftsFilepath.contains( "-y" ) ? "-y" : ( pairwiseShiftsFilepath.contains( "-z" ) ? "-z" : "" ) ) );

		int valid = 0;
		final String timePatternStr = ".*_(\\d*)msecAbs.*";
		final Pattern timePattern = Pattern.compile( timePatternStr );

		final String outFilepath = outputFolder+"/pairs" + suffixInliers + suffixDim + ".txt";

		final PrintWriter writer = new PrintWriter(outFilepath, "UTF-8");
		for ( final SerializablePairWiseStitchingResult shift : shifts )
		{
			final TileInfo[] tilePair = shift.getTileBoxPair().getOriginalTilePair().toArray();

			if ( tilePair[ 0 ].getIndex() > tilePair[ 1 ].getIndex() )
				System.out.println( "Indices are unsorted" );

			final double dist[] = new double[ shift.getNumDimensions() ];
			for ( int d = 0; d < shift.getNumDimensions(); d++ )
				dist[d] = (tilePair[ 1 ].getStagePosition( d ) - tilePair[ 0 ].getStagePosition( d )) - shift.getOffset( d );

			final long[] diffTime = new long[ channelsMap.length ];
			for ( int i = 0; i < channelsMap.length; i++ )
			{
				final long[] timestamps = new long[ 2 ];
				for ( int j = 0; j < 2; j++ )
				{
					final String filename = Paths.get( channelsMap[ i ].get( tilePair[ j ].getIndex() + i*tilesPerChannel ).getFilePath() ).getFileName().toString();
					final Matcher matcher = timePattern.matcher( filename );
					if ( !matcher.find() )
						throw new Exception( "Can't parse timestamp" );

					timestamps[ j ] = Long.parseLong( matcher.group( 1 ) );
				}

				if ( timestamps[ 0 ] > timestamps[ 1 ] )
					System.out.println( "Timestamps are unsorted" );

				diffTime[ i ] = Math.abs( timestamps[ 1 ] - timestamps[ 0 ] );
			}


			for ( int i = 1; i < diffTime.length; i++ )
				if ( diffTime[ i ] != diffTime[ i - 1 ] )
					throw new Exception( "Different time inverval between channels" );

			String diffTimeStr = "";
			if ( diffTime.length > 0 )
				diffTimeStr = " " + diffTime[ 0 ];

			if ( shift.getIsValidOverlap() )
			{
				valid++;
				writer.println( dist[0] + " " + dist[1] + " " + dist[2] + " " + shift.getCrossCorrelation() + diffTimeStr );
			}
		}
		writer.close();

		System.out.println( "Created: " + outFilepath );
		System.out.println( "Processed " + valid + " valid pairs out of " + shifts.size() );
	}
}
